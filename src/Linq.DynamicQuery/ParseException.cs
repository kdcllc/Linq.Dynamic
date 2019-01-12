namespace System.Linq.Dynamic
{
    public sealed class ParseException : Exception
    {
        public ParseException(string message, int position)
            : base(message)
        {
            Position = position;
        }

        public int Position { get; private set; }

        public override string ToString()
        {
            return string.Format(Res.ParseExceptionFormat, Message, Position);
        }

        public ParseException()
        {
        }

        public ParseException(string message) : base(message)
        {
        }

        public ParseException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
