using System;

namespace Open.Journaling.Azure
{
    public static class StringEncodingExtensions
    {
        /// <summary>
        ///     Escapes a common set of characters to alphanumeric only, utilizing some custom encoding
        /// 
        ///     In:     !	#	$	%	&	'	*	+	-	.	/	;	=	?	@	^	_	`	{	|	}	~
        ///     Out:    Z21	Z23	Z24	Z25	Z26	Z27	Z2A	Z2B	ZHY	ZDT	Z2F	Z3B	Z3D	Z3F	Z40	Z5E	ZUS	Z60	Z7B	Z7C	Z7D	ZTI
        /// </summary>
        /// <param name="value"></param>
        /// <returns>A lowercase representation of the string with custom encoding</returns>
        public static string AsCustomAlphaNumeric(
            this string value)
        {
            return
                Uri.EscapeDataString(
                        value.ToLowerInvariant())
                    .Replace("%", "Z")
                    .Replace(".", "ZDT")
                    .Replace("-", "ZHY")
                    .Replace("~", "ZTI")
                    .Replace("_", "ZUS");
        }

        public static string FromCustomAlpahNumeric(
            this string value)
        {
            return
                Uri.UnescapeDataString(
                    value
                        .Replace("ZDT", ".")
                        .Replace("ZHY", "-")
                        .Replace("ZTI", "~")
                        .Replace("ZUS", "_")
                        .Replace("Z", "%"));
        }
    }
}