namespace Open.Journaling.Azure.Entities
{
    public sealed class EntityType
    {
        public const string Internal = "_";
        public const string Entry = "e";
        public const string Tag = "t";
        public const string UtcTicks = "u";
        public const string Consistent = "c";
    }
}