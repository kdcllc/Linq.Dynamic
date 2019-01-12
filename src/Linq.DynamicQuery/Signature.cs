using System.Collections.Generic;

namespace System.Linq.Dynamic
{
    internal class Signature : IEquatable<Signature>
    {
        public DynamicProperty[] Properties;
        public int HashCode;

        public Signature(IEnumerable<DynamicProperty> properties)
        {
            Properties = properties.ToArray();
            HashCode = 0;
            foreach (var p in properties)
            {
                HashCode ^= p.Name.GetHashCode() ^ p.Type.GetHashCode();
            }
        }

        public override int GetHashCode()
        {
            return HashCode;
        }

        public override bool Equals(object obj)
        {
            return obj is Signature ? Equals((Signature)obj) : false;
        }

        public bool Equals(Signature other)
        {
            if (Properties.Length != other.Properties.Length)
            {
                return false;
            }

            for (var i = 0; i < Properties.Length; i++)
            {
                if (Properties[i].Name != other.Properties[i].Name ||
                    Properties[i].Type != other.Properties[i].Type)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
