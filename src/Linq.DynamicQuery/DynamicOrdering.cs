using System.Linq.Expressions;

namespace System.Linq.Dynamic
{
    internal class DynamicOrdering
    {
        public Expression Selector;
        public ParameterExpression Parameter;
        public bool Ascending;
    }
}
