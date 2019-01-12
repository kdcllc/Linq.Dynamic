namespace System.Linq.Dynamic
{
    public class DynamicProperty
    {
        public DynamicProperty(string name, Type type)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        public string Name { get; private set; }

        public Type Type { get; private set; }
    }
}
