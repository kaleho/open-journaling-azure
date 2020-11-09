using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Open.Journaling.Azure.Extensions;
using Open.Journaling.Traits;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Open.Journaling.Azure.Journals
{
    public sealed class JournalProvider
        : IJournalReaderProvider,
          IJournalWriterProvider
    {
        public const string Name = "azurejournalprovider";
        private readonly ConcurrentDictionary<JournalId, Lazy<Journal>> _cache;
        private readonly CloudTableClient _cloudTableClient;
        private readonly ILogger<JournalProvider> _log;
        private readonly ILoggerFactory _loggerFactory;

        public JournalProvider(
            ILoggerFactory loggerFactory,
            ProviderSettings settings)
        {
            _loggerFactory = loggerFactory;

            _log = _loggerFactory.CreateLogger<JournalProvider>();

            Settings = settings;

            var storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);

            _cloudTableClient = storageAccount.CreateCloudTableClient();

            _cache = new ConcurrentDictionary<JournalId, Lazy<Journal>>();

            ProviderId = ProviderId.For(Name);

            ConnectionString =
                $"ProviderId={Name};{Settings.ConnectionString}";
        }

        public ImmutableDictionary<JournalId, Lazy<IJournal>> Cache =>
            ImmutableDictionary.CreateRange(
                _cache.ToDictionary(
                    x => x.Key,
                    x => new Lazy<IJournal>(() => x.Value.Value)));

        public string ConnectionString { get; }

        public ProviderId ProviderId { get; }

        public ProviderSettings Settings { get; }

        public JournalTraits Traits => Settings.JournalTraits;

        public bool HasJournal(
            JournalId journalId)
        {
            var returnValue =
                _cache.ContainsKey(journalId) ||
                _cloudTableClient
                    .GetTableReference(journalId.AsTableName())
                    .Exists();

            return returnValue;
        }

        public bool HasTraits(
            IEnumerable<IJournalTrait> traits)
        {
            var returnValue = true;

            foreach (var trait in traits)
            {
                var currentTrait = Traits.Traits.FirstOrDefault(x => x.GetType() == trait.GetType());

                if (currentTrait == null ||
                    trait.Value != TriState.Indeterminate &&
                    trait.Value != currentTrait.Value)
                {
                    returnValue = false;

                    break;
                }
            }

            return returnValue;
        }

        public bool OwnsConnection(
            string connectionString)
        {
            var returnValue = false;

            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                var parsed = new JournalConnectionString(connectionString);

                returnValue = parsed.KeyHasValue("ProviderId", Name);
            }

            return returnValue;
        }

        public bool TryGetOrCreate(
            JournalId journalId,
            IEnumerable<IJournalTrait> traits,
            out IJournalReader reader)
        {
            reader =
                HasTraits(traits)
                    ? GetOrCreate(journalId)
                    : null;

            return reader != null;
        }

        public bool TryGetOrCreate(
            JournalId journalId,
            IEnumerable<IJournalTrait> traits,
            out IJournalWriter writer)
        {
            writer =
                HasTraits(traits)
                    ? GetOrCreate(journalId)
                    : null;

            return writer != null;
        }

        private Journal GetOrCreate(
            JournalId journalId)
        {
            var returnValue =
                _cache.GetOrAdd(
                    journalId,
                    x =>
                        new Lazy<Journal>(
                            () =>
                            {
                                _log.LogDebug("Creating {JournalId}.", journalId);

                                var journal =
                                    new Journal(
                                        _loggerFactory.CreateLogger<Journal>(),
                                        new JournalSettings(
                                            Settings.ConnectionString,
                                            Settings.ConnectionTimeout,
                                            Settings.InitializationTimeout,
                                            journalId.AsTableName(),
                                            journalId,
                                            Settings.WriterRetryLimit));

                                return journal;
                            })
                    );

            return returnValue.Value;
        }
    }
}