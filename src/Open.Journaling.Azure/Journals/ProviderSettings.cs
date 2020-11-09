using Open.Journaling.Traits;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Open.Journaling.Azure.Journals
{
    public sealed class ProviderSettings
    {
        public const int DefaultWriterRetryLimit = 5;

        public static ImmutableList<IJournalTrait> ImmutableProviderTraits =
            ImmutableList.CreateRange(
                new IJournalTrait[]
                {
                    new AtomicTrait(TriState.True),
                    new DurableTrait(TriState.True),
                    new AzureStorageTrait(TriState.True),
                });

        public ProviderSettings(
            string connectionString,
            int connectionTimeout,
            int initializationTimeout,
            IEnumerable<IJournalTrait> additionalTraits = null,
            int writerRetryLimit = DefaultWriterRetryLimit)
        {
            ConnectionString = connectionString;
            ConnectionTimeout = connectionTimeout;
            InitializationTimeout = initializationTimeout;
            WriterRetryLimit = writerRetryLimit;

            var traits = new List<IJournalTrait>(ImmutableProviderTraits);

            if (additionalTraits != null)
            {
                traits.AddRange(
                    additionalTraits.Where(
                        trait => ImmutableProviderTraits.All(x => trait.GetType() != x.GetType())));
            }

            JournalTraits = new JournalTraits(traits);
        }

        public string ConnectionString { get; }

        public int ConnectionTimeout { get; }

        public int InitializationTimeout { get; }

        public JournalTraits JournalTraits { get; }

        public int WriterRetryLimit { get; }
    }
}