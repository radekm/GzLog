- Consider using `ArrayBufferWriter` inside `membersFromStream` instead of `inputOwner` and `outputOwner`.
- In `LogReader` we use `maxDecompressedMemberLen` but in `LogWriter` we use `MaxDecompressedMemberSize`.
  One problem is that these two fields mean two different things.
  `maxDecompressedMemberLen` acts more like hard limit but `MaxDecompressedMemberSize`
  will be breached if single message is bigger. Another problem is that
  we are not consistent in naming and use different suffixes `Size` and `Len`.
