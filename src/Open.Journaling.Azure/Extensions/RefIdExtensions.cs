using Open.Monikers;

namespace Open.Journaling.Azure.Extensions
{
    public static class RefIdExtensions
    {
        /// <summary>
        /// </summary>
        /// <param name="refId"></param>
        /// <returns>A lowercase representation of the refId with custom encoding</returns>
        public static string AsTableName(
            this RefId refId)
        {
            return refId.ToString().AsCustomAlphaNumeric();
        }
    }
}