using System.Text.RegularExpressions;

namespace Open.Journaling.Azure.Journals
{
    public class JournalSettings
        : IJournalSettings
    {
        public const int DefaultWriterRetryLimit = 5;
        private static readonly Regex _regexp = new Regex("^[A-Za-z][A-Za-z0-9]{2,62}$");

        public JournalSettings(
            string connectionString,
            int connectionTimeout,
            int initializationTimeout,
            string tableName,
            JournalId journalId,
            int writerRetryLimit = DefaultWriterRetryLimit)
        {
            if (!IsValid(tableName))
            {
                throw new JournalSettingsException(
                    JournalSettingsException.DefaultMessage +
                    $" The table name [{tableName}] does not comply with requirements. Please see: " +
                    " https://docs.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model");
            }

            ConnectionString = connectionString;
            ConnectionTimeout = connectionTimeout;
            InitializationTimeout = initializationTimeout;
            TableName = tableName;
            JournalId = journalId;
            WriterRetryLimit = writerRetryLimit;
        }

        public string ConnectionString { get; }

        public int ConnectionTimeout { get; }

        public int InitializationTimeout { get; }

        /// <summary>
        ///     Table names may contain only alphanumeric characters.
        ///     Table names cannot begin with a numeric character.
        ///     Table names are case-insensitive.
        ///     Table names must be from 3 to 63 characters long.
        /// </summary>
        public string TableName { get; }

        public int WriterRetryLimit { get; }

        public JournalId JournalId { get; }

        private bool IsValid(
            string tableName)
        {
            var isValid = _regexp.IsMatch(tableName);

            return isValid;
        }
    }
}