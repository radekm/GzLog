module GzLog.Test.Writer

open System
open System.IO

open NUnit.Framework

open GzLog

type TempDirectory() =
    let mutable disposed = false
    let info = Directory.CreateTempSubdirectory("GzLogTest")

    member _.Path = info.FullName

    interface IDisposable with
        override _.Dispose() =
            if not disposed then
                try
                    info.Delete(true)
                // TODO Don't catch all exceptions - eg. `OutOfMemoryException`.
                with e ->
                    eprintfn "Delete during Dispose failed: %A" e

let config = { MaxDecompressedMemberSize = 5
               MaxFileSize = 1024
               MaxSecondsBeforeFlush = 60
               DirFunc = fun _ -> failwith "Missing DirFunc" }

let dt = DateTimeOffset(2023, 1, 29, 9, 0, 0, TimeSpan.Zero)

type MemberData = byte array
type FileName = string
type DirName = string

let readMembers (file : string) : MemberData list =
    use stream = new FileStream(file, FileMode.Open)
    let n = 1024 * 1024
    LogReader.membersFromStream stream n n
    |> Seq.map (fun m -> m.Decompressed.ToArray())
    |> Seq.toList

let checkLogs (baseDir : string) (expected : list<DirName * FileName * MemberData list>) =
    let expected =
        expected
        |> List.groupBy (fun (dir, _, _) -> dir)
        |> List.map (fun (dir, filesInDir) ->
            filesInDir
            |> List.map (fun (_, file, members) -> file, members)
            |> Map.ofList
            |> fun map -> dir, map)
        |> Map.ofList

    CollectionAssert.IsEmpty(Directory.GetFiles(baseDir), "Files in base dir")
    // Order of dirs is not important.
    CollectionAssert.AreEquivalent(
        expected.Keys,
        Directory.GetDirectories(baseDir) |> Array.map Path.GetFileName,
        "Directories in base dir")

    expected
    |> Map.iter (fun dir filesInDir ->
        let path = Path.Combine(baseDir, dir)

        CollectionAssert.IsEmpty(Directory.GetDirectories(path), $"Directories in %s{dir}")
        // Order of files is not important.
        CollectionAssert.AreEquivalent(
            filesInDir.Keys,
            Directory.GetFiles(path) |> Array.map Path.GetFileName,
            $"Files in %s{dir}")

        filesInDir
        |> Map.iter (fun file members ->
            let path = Path.Combine(path, file)
            // Order of members is important.
            CollectionAssert.AreEqual(members, readMembers path, $"Members of %s{dir}/%s{file}")))

[<Test>]
let ``date in path is greater or equal than date in message timestamp`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    // Must end in `2023-01-29`.
    writer.AppendMessage(dt, "A"B)
    // Must end in `2023-01-30`.
    writer.AppendMessage(dt.AddDays 1, "B"B)
    writer.AppendMessage(dt, "C"B)
    writer.AppendMessage(dt.AddDays 1, "D"B)
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "A"B ]
          "2023-01-30", "log_090000.gz", [ "BCD"B ] ]

[<Test>]
let ``capacity of member is not exceeded`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "AB"B)
    writer.AppendMessage(dt, "CD"B)
    writer.AppendMessage(dt, "EF"B)  // Goes to new member.
    writer.AppendMessage(dt, "GH"B)
    writer.AppendMessage(dt, "I"B)
    writer.AppendMessage(dt, "J"B)  // Goes to new member.
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "ABCD"B; "EFGHI"B; "J"B ] ]

[<Test>]
let ``capacity of member can be exceeded by single oversized message`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "A"B)
    writer.AppendMessage(dt, "BCDEFGHI"B)  // This message is oversized and must be alone in single member.
    writer.AppendMessage(dt, "J"B)
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "A"B; "BCDEFGHI"B; "J"B ] ]

[<Test>]
let ``capacity of file is not exceeded`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    // Create huge data which after compression fits 2 times into single file.
    // FIXME: This test is fragile and depends on compression ratio which could change.
    let random = Random(20)
    let hugeData = Array.init 480 (fun _ -> random.Next() |> byte)

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, hugeData)
    writer.AppendMessage(dt.AddSeconds 1, hugeData)
    writer.AppendMessage(dt.AddSeconds 1, hugeData)  // Goes to new file.
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ hugeData; hugeData ]
          "2023-01-29", "log_090001.gz", [ hugeData ] ]

[<Test>]
let ``capacity of file can be exceeded by single oversized message`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    // Create huge data which after compression doesn't fit into file capacity.
    // FIXME: This test is fragile and depends on compression ratio which could change.
    let random = Random(20)
    let hugeData = Array.init (int config.MaxFileSize) (fun _ -> random.Next() |> byte)

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "A"B)
    writer.AppendMessage(dt.AddSeconds 1, hugeData)  // Goes to new file.
    writer.AppendMessage(dt.AddSeconds 2, "Z"B)  // Goes to new file.
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "A"B ]
          "2023-01-29", "log_090001.gz", [ hugeData ]
          "2023-01-29", "log_090002.gz", [ "Z"B ] ]

[<Test>]
let ``files are ordered lexicographically`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    // Create huge data which after compression doesn't fit into file capacity.
    // FIXME: This test is fragile and depends on compression ratio which could change.
    let random = Random(20)
    let hugeData = Array.init (int config.MaxFileSize) (fun _ -> random.Next() |> byte)

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, hugeData)
    writer.AppendMessage(dt, hugeData)  // Goes to new file.
    writer.AppendMessage(dt, hugeData)  // Goes to new file.
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ hugeData ]
          "2023-01-29", "log_090000x.gz", [ hugeData ]  // `x` must be appended because timestamp is same.
          "2023-01-29", "log_090000xx.gz", [ hugeData ] ]

[<Test>]
let ``dispose flushes data`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    let _ =
        use writer = new LogWriter(config)
        writer.AppendMessage(dt, "AB"B)

        // Ensure that data were not flushed.
        checkLogs baseDir.Path []

        // Dispose is called here.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "AB"B ] ]

[<Test>]
let ``empty message is ignored`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, ""B)
    writer.FlushMember()

    checkLogs baseDir.Path []

// It's expected that this test prints info about the exception on stderr.
[<Test>]
let ``dispose doesn't throw exception`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    Assert.DoesNotThrow(fun () ->
        use writer = new LogWriter(config)
        writer.AppendMessage(dt, "A"B)

        // We want to test a situation when `Dispose` cannot flush buffered messages.
        // To ensure that `Dispose` cannot create the log file
        // we create a directory with the name as the log file.
        Directory.CreateDirectory(Path.Combine(baseDir.Path, "2023-01-29/log_090000.gz")) |> ignore)

[<Test>]
let ``message with big enough timestamp causes flush`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "A"B)
    // No flush because of timestamp since `MaxSecondsBeforeFlush` wasn't exceeded.
    writer.AppendMessage(dt.AddSeconds config.MaxSecondsBeforeFlush, "B"B)
    // Causes flush after appending message to member.
    writer.AppendMessage(dt.AddSeconds(float config.MaxSecondsBeforeFlush + 1.0), "C"B)
    writer.AppendMessage(dt.AddSeconds(float config.MaxSecondsBeforeFlush + 1.0), "D"B)  // Goes to new member.
    writer.FlushMember()  // We need to flush before checking.

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "ABC"B; "D"B ] ]

[<Test>]
let ``FlushMemberIfTooOld doesn't flush if member isn't too old`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "A"B)
    writer.FlushMemberIfTooOld(dt.AddSeconds(float config.MaxSecondsBeforeFlush / 2.0))
    writer.FlushMemberIfTooOld(dt.AddSeconds config.MaxSecondsBeforeFlush)

    checkLogs baseDir.Path []  // Nothing was flushed.

[<Test>]
let ``FlushMemberIfTooOld flushes too old member`` () =
    use baseDir = new TempDirectory()
    let config = { config with DirFunc = fun date -> Path.Combine(baseDir.Path, date) }

    use writer = new LogWriter(config)
    writer.AppendMessage(dt, "A"B)
    writer.FlushMemberIfTooOld(dt.AddSeconds(float config.MaxSecondsBeforeFlush + 1.0))

    checkLogs baseDir.Path
        [ "2023-01-29", "log_090000.gz", [ "A"B ] ]
