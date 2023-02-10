namespace GzLog

open System
open System.Buffers
open System.IO

open LibDeflateGzip

/// Has no path and no extension.
type private FileWithoutExt = string

type private FileState =
    { DateInPath : string
      Name : FileWithoutExt
      Size : int64
    }

type private DecompressedMemberState =
    { Timestamp : DateTimeOffset
      Size : int
    }

type private LogWriterState =
    | Initial
    | EmptyMember of FileState
    // Non-empty member has decompressed member size > 0.
    // If message timestamps are monotone then all messages in member belong to date `DateInPath`.
    // Otherwise all messages in member belong to date `DateInPath` or earlier date.
    | NonEmptyMember of FileState * DecompressedMemberState

type LogWriterConfig =
    { // Member will contain at least one message and messages are never split into more members.
      // This means that `MaxDecompressedMemberSize` limit will be breached if the message is bigger.
      MaxDecompressedMemberSize : int
      // This is compressed size. File will contain at least one member and members are never split into more files.
      // This means that `MaxFileSize` limit will be breached if compressed member is bigger.
      MaxFileSize : int64
      MaxSecondsBeforeFlush : int
      // Pure function. Must be injective.
      DirFunc : string -> string
    }

[<Sealed>]
type LogWriter(config : LogWriterConfig) =
    let filePrefix = "log_"
    let dataExtension = ".gz"
    let sizeExtension = ".last-size"

    let mutable disposed = false
    // Set to false before appending to files.
    // If appending fails due to an exception in the middle then `LogWriter`
    // is in uncertain state. It doesn't know if some or all data were written
    // to file and whether the file isn't corrupted.
    // Fortunately if such situation happens `lastAppendFinished` remains set to false
    // and `LogWriter` becomes unusable.
    let mutable lastAppendFinished = true

    let mutable state = Initial

    let mutable decompressedMemberBuffer = MemoryPool.Shared.Rent(2 * 1024 * 1024)
    let mutable compressedMemberBuffer = MemoryPool.Shared.Rent(2 * 1024 * 1024)  // Consider making size configurable.
    let compressor = new Compressor(12)  // Consider making compression level configurable.

    let check () =
        if not lastAppendFinished then
            failwith "Last append haven't finished"
        if disposed then
            raise (ObjectDisposedException(nameof LogWriter))

    let timestampToDate (timestamp : DateTimeOffset) =
        timestamp.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)

    let timestampToFile (timestamp : DateTimeOffset) : FileWithoutExt =
        filePrefix + timestamp.ToString("HHmmss", System.Globalization.CultureInfo.InvariantCulture)

    // Returns file names with extension.
    let listLogs dir =
        try
            Directory.GetFiles dir
            |> Array.map Path.GetFileName
            |> Array.filter (fun file ->
                let extOk =
                    file.EndsWith(dataExtension, StringComparison.InvariantCultureIgnoreCase) ||
                    file.EndsWith(sizeExtension, StringComparison.InvariantCultureIgnoreCase)
                extOk && file.StartsWith(filePrefix, StringComparison.InvariantCultureIgnoreCase))
        with :? DirectoryNotFoundException -> [||]

    let getLexicographicallyBiggerFile (timestamp : DateTimeOffset) (lastFile : FileWithoutExt) : FileWithoutExt =
        let newFile = timestampToFile timestamp
        if lastFile < newFile
        then newFile
        else lastFile + "x"  // Create different name which is lexicographically AFTER.

    let createInitialFileState (timestamp : DateTimeOffset) : FileState =
        let today = timestampToDate timestamp
        let tomorrow = timestampToDate (timestamp.AddDays 1)

        let dateInPath, existingLogs =
            match listLogs (config.DirFunc tomorrow) with
            | [||] -> today, listLogs (config.DirFunc today)
            | logs -> tomorrow, logs

        let file =
            match existingLogs with
            | [||] -> timestampToFile timestamp
            | logs ->
                logs
                |> Seq.map Path.GetFileNameWithoutExtension
                |> Seq.max
                |> getLexicographicallyBiggerFile timestamp
        { DateInPath = dateInPath
          Name = file
          Size = 0L
        }

    member _.FlushMember() =
        check ()

        match state with
        | Initial | EmptyMember _ -> ()
        | NonEmptyMember (fileState, decompressedMemberState) ->
            // Compress member.
            let mutable compressedMemberSize = 0
            while compressedMemberSize = 0 do
                compressedMemberSize <-
                    compressor.Compress(
                        Span.op_Implicit(decompressedMemberBuffer.Memory.Span.Slice(0, decompressedMemberState.Size)),
                        compressedMemberBuffer.Memory.Span)
                // `compressedMemberBuffer` wasn't big enough.
                if compressedMemberSize = 0 then
                    let newSize = 2 * compressedMemberBuffer.Memory.Length
                    compressedMemberBuffer.Dispose()
                    compressedMemberBuffer <- MemoryPool.Shared.Rent(newSize)

            let fileState =
                // If the file is empty then we use it.
                // Otherwise we check whether member fits into file
                // and if not then we use another file.
                if fileState.Size > 0 && config.MaxFileSize - fileState.Size < compressedMemberSize then
                    { DateInPath = fileState.DateInPath
                      Name = getLexicographicallyBiggerFile decompressedMemberState.Timestamp fileState.Name
                      Size = 0L
                    }
                else fileState

            let dir = config.DirFunc fileState.DateInPath
            let file = Path.Combine(dir, fileState.Name)
            let sizeFile = file + sizeExtension
            let dataFile = file + dataExtension

            lastAppendFinished <- false
            Directory.CreateDirectory(dir) |> ignore
            File.WriteAllText(sizeFile, string fileState.Size)

            // Append compressed member to file.
            let _ =
                use stream = new FileStream(dataFile, FileMode.Append)
                stream.Write(compressedMemberBuffer.Memory.Span.Slice(0, compressedMemberSize))
            // Do we need to flush parent directory (especially if the file was newly created)?

            File.Delete(sizeFile)
            state <- EmptyMember { fileState with Size = fileState.Size + int64 compressedMemberSize }
            lastAppendFinished <- true

    /// Ensures that we don't hold old messages in memory for too long.
    /// This function is automatically called after appending message.
    member me.FlushMemberIfTooOld(timestamp : DateTimeOffset) =
        check ()

        match state with
        | Initial | EmptyMember _ -> ()
        | NonEmptyMember (_fileState, decompressedMemberState) ->
            if timestamp - decompressedMemberState.Timestamp > TimeSpan.FromSeconds config.MaxSecondsBeforeFlush then
                me.FlushMember()

    member me.AppendMessage(timestamp : DateTimeOffset, message : ReadOnlySpan<byte>) =
        check ()

        if not message.IsEmpty then
            me.AppendNonEmptyMessage(timestamp, message)

    member private me.AppendNonEmptyMessage(timestamp : DateTimeOffset, message : ReadOnlySpan<byte>) =
        if message.IsEmpty then
            failwith "Empty message"

        // Ensure we're in correct directory.
        match state with
        | Initial -> state <- EmptyMember (createInitialFileState timestamp)
        | EmptyMember fileState | NonEmptyMember (fileState, _) ->
            let newDateInPath = timestampToDate timestamp

            // We have to select another dir.
            if fileState.DateInPath < newDateInPath then
                me.FlushMember()  // We have to flush BEFORE changing directory.

                // Flush won't change `DateInPath`.
                // So we don't need to reread `state` after flushing because we're changing `DateInPath`.
                state <- EmptyMember { DateInPath = newDateInPath
                                       Name = timestampToFile timestamp
                                       Size = 0L
                                     }

        // Ensure that we don't exceed maximum member size.
        let decompressedMemberState =
            match state with
            | Initial -> failwith "Absurd"
            | EmptyMember _fileState -> { Timestamp = timestamp; Size = 0 }
            | NonEmptyMember (_fileState, decompressedMemberState) ->
                if config.MaxDecompressedMemberSize - decompressedMemberState.Size < message.Length then
                    me.FlushMember()  // Flush preserves `DateInPath` but may change file's `Name` and `Size`.
                    { Timestamp = timestamp; Size = 0 }
                else
                    decompressedMemberState

        let newSize = decompressedMemberState.Size + message.Length
        // We have to enlarge `decompressedMemberBuffer`.
        if decompressedMemberBuffer.Memory.Length < newSize then
            use mutable newDecompressedMemberBuffer = MemoryPool.Shared.Rent(newSize)
            let old = decompressedMemberBuffer
            old.Memory.Slice(0, decompressedMemberState.Size).CopyTo(newDecompressedMemberBuffer.Memory)
            decompressedMemberBuffer <- newDecompressedMemberBuffer
            newDecompressedMemberBuffer <- old  // This line guarantees that old buffer will be freed.

        message.CopyTo(decompressedMemberBuffer.Memory.Span.Slice(decompressedMemberState.Size))

        // We have to read `fileState` AFTER all calls to `FlushMember`
        // because these calls may change file's `Name` and `Size`.
        let fileState =
            match state with
            | Initial -> failwith "Absurd"
            | EmptyMember fileState | NonEmptyMember (fileState, _) -> fileState
        state <- NonEmptyMember (fileState, { Timestamp = decompressedMemberState.Timestamp; Size = newSize })

        me.FlushMemberIfTooOld(timestamp)

    interface IDisposable with
        override me.Dispose() =
            if not disposed then
                try
                    // Flush only if everything was alright.
                    if lastAppendFinished then
                        me.FlushMember()
                // TODO Don't catch all exceptions - eg. `OutOfMemoryException`.
                with e ->
                    eprintfn "FlushMember during Dispose failed: %A" e

                disposed <- true
                decompressedMemberBuffer.Dispose()
                compressedMemberBuffer.Dispose()
                compressor.Dispose()
