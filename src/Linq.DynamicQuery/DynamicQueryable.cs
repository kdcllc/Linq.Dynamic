using System.Linq.Expressions;
using System.Reflection;

namespace System.Linq.Dynamic
{
    public static class DynamicQueryable
    {
        #region Where
        public static IQueryable<T> Where<T>(this IQueryable<T> source, string predicate, params object[] values)
        {
            return (IQueryable<T>)Where((IQueryable)source, predicate, values);
        }

        public static IQueryable Where(this IQueryable source, string predicate, params object[] values)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }

            var lambda = DynamicExpression.ParseLambda(source.ElementType, typeof(bool), predicate, values);
            return source.Provider.CreateQuery(
                Expression.Call(
                    typeof(Queryable), "Where",
                    new Type[] { source.ElementType },
                    source.Expression, Expression.Quote(lambda)));
        }
        #endregion

        #region Select/SelectMany
        public static IQueryable<T> Select<T>(this IQueryable<T> source, string selector, params object[] values)
        {
            return (IQueryable<T>)Select((IQueryable)source, selector, values);
        }

        public static IQueryable Select(this IQueryable source, string selector, params object[] values)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selector == null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            var lambda = DynamicExpression.ParseLambda(source.ElementType, null, selector, values);
            return source.Provider.CreateQuery(
                Expression.Call(
                    typeof(Queryable), "Select",
                    new Type[] { source.ElementType, lambda.Body.Type },
                    source.Expression, Expression.Quote(lambda)));
        }

        public static IQueryable SelectMany(this IQueryable source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Provider.CreateQuery(
                Expression.Call(
                typeof(Queryable), "SelectMany",
                new Type[] { source.ElementType },
                source.Expression));
        }

        #endregion

        public static IQueryable First(this IQueryable source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Provider.CreateQuery(
                Expression.Call(
                typeof(Queryable), "First",
                new Type[] { source.ElementType },
                source.Expression));
        }

        public static IQueryable Distinct(this IQueryable source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Provider.CreateQuery(
                    Expression.Call(
                    typeof(Queryable), "Distinct",
                    new Type[] { source.ElementType },
                    source.Expression));
        }

        public static IQueryable Union(this IQueryable source1, IQueryable source2)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException(nameof(source1));
            }

            if (source2 == null)
            {
                throw new ArgumentNullException(nameof(source2));
            }

            return source1.Provider.CreateQuery(
                    Expression.Call(
                    typeof(Queryable), "Union",
                    new Type[] { source1.ElementType },
                    source1.Expression, source2.Expression));
        }

        public static IQueryable Concat(this IQueryable source1, IQueryable source2)
        {
            if (source1 == null)
            {
                throw new ArgumentNullException(nameof(source1));
            }

            if (source2 == null)
            {
                throw new ArgumentNullException(nameof(source2));
            }

            return source1.Provider.CreateQuery(
                 Expression.Call(
                typeof(Queryable), "Concat",
                 new Type[] { source1.ElementType },
                 source1.Expression, source2.Expression));
        }

        #region OrderBy
       
        public static IQueryable<T> OrderBy<T>(this IQueryable<T> source, string ordering, params object[] values)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (ordering == null)
            {
                throw new ArgumentNullException(nameof(ordering));
            }
            var parameters = new[]
            {
                Expression.Parameter(source.ElementType, "")
            };

            var parser = new ExpressionParser(parameters, ordering, values);
            var orderings = parser.ParseOrdering();

            object result = source;

            var helperMethodName = "OrderByHelper";
            foreach (var order in orderings)
            {
                var keySelectorExpression = Expression.Lambda(
                    order.Selector,
                    order.Parameter
                );

                var orderByHelperMethod = typeof(DynamicQueryable)
                    .GetMethod(helperMethodName, BindingFlags.NonPublic | BindingFlags.Static)
                    .MakeGenericMethod(typeof(T), order.Selector.Type);

                result = orderByHelperMethod.Invoke(null, new object[] { result, keySelectorExpression, order.Ascending });

                helperMethodName = "ThenByHelper";
            }

            return (IOrderedQueryable<T>)result;
        }

        private static IOrderedQueryable<T> OrderByHelper<T, TKey>(IQueryable<T> source, Expression<Func<T, TKey>> keySelector, bool ascending)
        {
            return ascending
                ? source.OrderBy(keySelector)
                : source.OrderByDescending(keySelector);
        }

        private static IOrderedQueryable<T> ThenByHelper<T, TKey>(IOrderedQueryable<T> source, Expression<Func<T, TKey>> keySelector, bool ascending)
        {
            return ascending
                ? source.ThenBy(keySelector)
                : source.ThenByDescending(keySelector);
        }

        #endregion

        #region GroupBy
        public static IQueryable<T> GroupBy<T>(this IQueryable<T> source, string keySelector, string elementSelector, params object[] values)
        {
            return (IQueryable<T>)GroupBy((IQueryable)source, keySelector, elementSelector, values);
        }

        public static IQueryable GroupBy(this IQueryable source, string keySelector, string elementSelector, params object[] values)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }

            if (elementSelector == null)
            {
                throw new ArgumentNullException(nameof(elementSelector));
            }

            var keyLambda = DynamicExpression.ParseLambda(source.ElementType, null, keySelector, values: values);
            var elementLambda = DynamicExpression.ParseLambda(source.ElementType, null, elementSelector, values: values);
            return source.Provider.CreateQuery(
                Expression.Call(
                    typeof(Queryable), "GroupBy",
                    new Type[] { source.ElementType, keyLambda.Body.Type, elementLambda.Body.Type },
                    source.Expression, Expression.Quote(keyLambda), Expression.Quote(elementLambda)));
        }


        #endregion
        public static IQueryable Take(this IQueryable source, int count)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Provider.CreateQuery(
                Expression.Call(
                    typeof(Queryable), "Take",
                    new Type[] { source.ElementType },
                    source.Expression, Expression.Constant(count)));
        }

        public static IQueryable Skip(this IQueryable source, int count)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Provider.CreateQuery(
                Expression.Call(
                    typeof(Queryable), "Skip",
                    new Type[] { source.ElementType },
                    source.Expression, Expression.Constant(count)));
        }


        public static bool Any(this IQueryable source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return (bool)source.Provider.Execute(
                Expression.Call(
                    typeof(Queryable), "Any",
                    new Type[] { source.ElementType }, source.Expression));
        }

        public static int Count(this IQueryable source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return (int)source.Provider.Execute(
                Expression.Call(
                    typeof(Queryable), "Count",
                    new Type[] { source.ElementType }, source.Expression));
        }
    }
}
