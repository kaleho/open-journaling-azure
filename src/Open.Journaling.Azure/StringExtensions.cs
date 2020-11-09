using System;
using System.Linq;

namespace Open.Journaling.Azure
{
    public static class StringExtensions
    {
        public static string FromSafeString(
            this string value)
        {
            return Uri.UnescapeDataString(value);
        }

        public static string[] FromSafeStrings(
            this string[] values)
        {
            return values.Select(FromSafeString).ToArray();
        }

        public static string ToSafeString(
            this string value)
        {
            return Uri.EscapeDataString(value);
        }

        public static string[] ToSafeStrings(
            this string[] values)
        {
            return values.Select(ToSafeString).ToArray();
        }
    }
}
