using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Open.Journaling.Azure.Journals;
using Open.Journaling.Model;
using Open.Monikers;
using Xunit;
using Xunit.Abstractions;

namespace Open.Journaling.Azure.Tests
{
    public class JournalPerformanceTests
        : IClassFixture<StorageFixture>
    {
        public JournalPerformanceTests(
            ITestOutputHelper testOutputHelper,
            StorageFixture storageFixture)
        {
            _testOutputHelper = testOutputHelper;
            _storageFixture = storageFixture;
        }

        private readonly StorageFixture _storageFixture;
        private readonly ITestOutputHelper _testOutputHelper;

        [Fact(Skip = "Performance test")]
        public void Can_Batch_Load_Items_x10000()
        {
            var stopwatch = Stopwatch.StartNew();

            var journalId = new JournalId("test");

            var tableName = $"t{Guid.NewGuid():N}";

            var tableInitializationStart = stopwatch.ElapsedMilliseconds;

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        _storageFixture.ConnectionString,
                        3000,
                        30000,
                        tableName,
                        journalId));

            var tableInitializationStop = stopwatch.ElapsedMilliseconds;

            var journalEntries = new List<IJournalEntry>();

            var entryInsertStart = stopwatch.ElapsedMilliseconds;

            var entries = new List<TestEntry>();

            for (var i = 0; i < 10000; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                entries.Add(entry);

                if (i % 25 == 0 || i == 9999)
                {
                    journalEntries.AddRange(
                        journal.Write(
                                new CancellationToken(),
                                entries.Select(
                                        x => new SerializedEntry(
                                            x.EntryId, new byte[0],
                                            new byte[0]))
                                    .ToArray())
                            .ConfigureAwait(false)
                            .GetAwaiter()
                            .GetResult());

                    entries.Clear();
                }
            }

            var entryInsertStop = stopwatch.ElapsedMilliseconds;

            _storageFixture.CleanupCloudTable(tableName)
                .Wait(TimeSpan.FromSeconds(3));

            stopwatch.Stop();

            _testOutputHelper.WriteLine(
                $"Table initialization: {tableInitializationStop - tableInitializationStart}ms");

            _testOutputHelper.WriteLine($"Entry insert: {entryInsertStop - entryInsertStart}ms");

            _testOutputHelper.WriteLine(
                $"{journalEntries.Count} entries were created, HighestSequenceNumber is {journal.Props.HighestSequenceNumber}");

            Assert.Equal(journalEntries.Count, journal.Props.HighestSequenceNumber);
        }

        [Fact(Skip = "Performance test")]
        public void Can_Load_Items_x10000()
        {
            var stopwatch = Stopwatch.StartNew();

            var journalId = new JournalId("test");

            var tableName = $"t{Guid.NewGuid():N}";

            var tableInitializationStart = stopwatch.ElapsedMilliseconds;

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        _storageFixture.ConnectionString,
                        3000,
                        30000,
                        tableName,
                        journalId));

            var tableInitializationStop = stopwatch.ElapsedMilliseconds;

            var journalEntries = new List<IJournalEntry>();

            var entryInsertStart = stopwatch.ElapsedMilliseconds;

            for (var i = 0; i < 10000; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());
            }

            var entryInsertStop = stopwatch.ElapsedMilliseconds;

            _storageFixture.CleanupCloudTable(tableName)
                .Wait(TimeSpan.FromSeconds(3));

            stopwatch.Stop();

            _testOutputHelper.WriteLine(
                $"Table initialization: {tableInitializationStop - tableInitializationStart}ms");

            _testOutputHelper.WriteLine($"Entry insert: {entryInsertStop - entryInsertStart}ms");

            _testOutputHelper.WriteLine(
                $"{journalEntries.Count} entries were created, HighestSequenceNumber is {journal.Props.HighestSequenceNumber}");

            Assert.Equal(journalEntries.Count, journal.Props.HighestSequenceNumber);
        }
    }
}