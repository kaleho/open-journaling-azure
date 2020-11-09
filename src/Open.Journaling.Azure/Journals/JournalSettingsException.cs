using System;

namespace Open.Journaling.Azure.Journals
{
    public class JournalSettingsException
        : Exception
    {
        public const string DefaultMessage =
            "There is an error with the provided journal settings.";

        public JournalSettingsException()
            : this(DefaultMessage)
        {
        }

        public JournalSettingsException(
            string message)
            : base(message)
        {
        }

        public JournalSettingsException(
            string message,
            Exception innerException)
            : base(message, innerException)
        {
        }
    }
}