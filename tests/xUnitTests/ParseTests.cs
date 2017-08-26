using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic;
using System.Linq.Expressions;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
using DynamicExpression = System.Linq.Dynamic.DynamicExpression;

namespace xUnitTests
{
    public class ParseTests
    {
        #region xUnit output setup
        private readonly ITestOutputHelper output;

        public ParseTests(ITestOutputHelper output)
        {
            this.output = output;

        }
        #endregion

        [Fact]
        public void ParseSimpleExpressionWorks()
        {
            var expression = @"x.Length == 4";

            var expr = (Expression<Func<string, bool>>)DynamicExpression
                        .ParseLambda(new[] { Expression.Parameter(typeof(string), "x") }, typeof(bool), expression);

            Assert.NotNull(expr);

            var values = new[] { "bar", "dog", "food", "water" }.AsQueryable();

            var results = values.Where(expr).ToList();

            results.Count.ShouldBe(1);
            results[0].ShouldBe("food");
        }

        [Fact]
        public void ParseSubQueryExpressionWorks()
        {
            var expression = "x.Any(it == 'a')";

            var expr = (Expression<Func<IEnumerable<char>, bool>>)DynamicExpression
                .ParseLambda(new[] { Expression.Parameter(typeof(IEnumerable<char>), "x") }, typeof(bool), expression);

            Assert.NotNull(expr);

            var values = new[] { "bar", "dog", "food", "water" }.AsQueryable();

            var results = values.Where(expr).ToList();

            results.Count.ShouldBe(2);
            results[0].ShouldBe("bar");
            results[1].ShouldBe("water");
        }

        [Fact]
        public void AccessEnumInExpressionWorks()
        {
            var expression = "it == Int32(MyEnum.Yes)";

            var expr = (Expression<Func<int, bool>>)DynamicExpression
                    .ParseLambda(typeof(int), typeof(bool), expression, additionalAllowedTypes: new[] { typeof(MyEnum) });

            expr.ShouldNotBeNull();

            var func = expr.Compile();

            func((int)MyEnum.Yes).ShouldBeTrue();
            func((int)MyEnum.No).ShouldBeFalse();
        }

        [Fact]
        public void AccessibleTypesCanBeSpecifiedOnEachCall()
        {
            DynamicExpression
                .ParseLambda(typeof(int), typeof(bool), "it != null");
            DynamicExpression.
                ParseLambda(typeof(int), typeof(bool), "it == Int32(MyEnum.Yes)", additionalAllowedTypes: new[] { typeof(MyEnum) });
        }

        #region Enum
        [Fact]
        public void EnumWithoutCastIsConvertedToDestinationType()
        {
            var expression = "it == MyEnum.Yes";

            var expr = (Expression<Func<int, bool>>)DynamicExpression
                .ParseLambda(typeof(int), typeof(bool), expression, additionalAllowedTypes: new[] { typeof(MyEnum) });

            output.WriteLine("{0}",expr);

            expr.ShouldNotBeNull();

            var func = expr.Compile();

            func((int)MyEnum.Yes).ShouldBeTrue();
            func((int)MyEnum.No).ShouldBeFalse();
        }

        [Fact]
        public void EnumWithoutCastIsConvertedFromInt32ToInt64()
        {
            var expression = "it == MyEnum.Yes";

            var expr = (Expression<Func<long, bool>>)DynamicExpression
                .ParseLambda(typeof(long), typeof(bool), expression, additionalAllowedTypes: new[] { typeof(MyEnum) });
            expr.ShouldNotBeNull();

            var func = expr.Compile();

            func((int)MyEnum.Yes).ShouldBeTrue();
            func((int)MyEnum.No).ShouldBeFalse();
        }
        #endregion

        [Fact]
        public void CanParseFirstOrDefaultExpression()
        {
            var expression = "FirstOrDefault(it == \"2\")";

            var expr = (Expression<Func<IEnumerable<string>, string>>)DynamicExpression
                .ParseLambda(typeof(IEnumerable<string>), typeof(string), expression);

            output.WriteLine("{0}",expr);

            Assert.NotNull(expr);

            var func = expr.Compile();

            func(new[] { "1", "2", "3" }).ShouldBe("2");
            func(new[] { "4" }).ShouldBeNull();
        }

        [Fact]
        public void CanParseFirstExpression()
        {
            var expression = "First(it == \"2\")";

            var expr = (Expression<Func<IEnumerable<string>, string>>)DynamicExpression
                .ParseLambda(typeof(IEnumerable<string>), typeof(string), expression);

            output.WriteLine("{0}", expr);

            Assert.NotNull(expr);

            var func = expr.Compile();
            var result = func(new[] { "1", "2", "3" });

            output.WriteLine(result);

            result.ShouldBe("2");
           
        }

               
        [Theory]
        [InlineData(new object[] { "1", "2", "3" },"1")]
        [InlineData(new object[] { "2", "2", "3" }, "2")]
        [InlineData(new object[] { }, null)]

        public void CanParseFirstOrDefaultExpressionWithoutParams(object[] a,object expectedesult)
        {
            var expression = "FirstOrDefault()";

            var expr = (Expression<Func<IEnumerable<object>, object>>)DynamicExpression
                .ParseLambda(typeof(IEnumerable<object>), typeof(object), expression);

            output.WriteLine("{0}",expr);

            expr.ShouldNotBeNull();

            var func = expr.Compile();
            var result = func(a);
            result.ShouldBe(expectedesult);
        }

        [Fact]
        public void CanParseNestedLambdasWithOuterVariableReference()
        {
            var expression = "resource.Any(allowed.Contains(it_1.Item1))";

            var parameters = new[]
            {
                Expression.Parameter(typeof(Tuple<string>[]), "resource"),
                Expression.Parameter(typeof(string[]), "allowed"),
            };

            var expr = (Expression<Func<Tuple<string>[], string[], bool>>)DynamicExpression
                .ParseLambda(parameters, typeof(bool), expression);

            Console.WriteLine(expr);

            Assert.NotNull(expr);

            var func = expr.Compile();

            Assert.True(func(new[] { Tuple.Create("1"), Tuple.Create("2") }, new[] { "1", "3" }));
            Assert.False(func(new[] { Tuple.Create("1"), Tuple.Create("2") }, new[] { "3" }));
        }

        [Fact]
        public void CanParseAs()
        {
            var expression = "(resource as System.String).Length";

            var parameters = new[]
            {
                Expression.Parameter(typeof(object), "resource"),
            };

            var expr = (Expression<Func<object, int>>)DynamicExpression
                .ParseLambda(parameters, typeof(int), expression);

            Console.WriteLine(expr);

            Assert.NotNull(expr);

            var func = expr.Compile();

            Assert.Equal(5, func("hello"));
        }

        [Fact]
        public void CanParseIs()
        {
            var expression = "resource is System.String";

            var parameters = new[]
            {
                Expression.Parameter(typeof(object), "resource"),
            };

            var expr = (Expression<Func<object, bool>>)DynamicExpression
                .ParseLambda(parameters, typeof(bool), expression);

            Console.WriteLine(expr);

            Assert.NotNull(expr);

            var func = expr.Compile();

            Assert.True(func("hello"));
            Assert.False(func(2));
        }

        [Fact]
        public void CanParseNew()
        {
            var expression = "new(resource.Length alias Len)";

            var parameters = new[]
            {
                Expression.Parameter(typeof(string), "resource"),
            };

            var expr = (Expression<Func<string, object>>)DynamicExpression
                .ParseLambda(parameters, typeof(object), expression);

            Console.WriteLine(expr);

            Assert.NotNull(expr);

            var func = expr.Compile();
        }

        [Fact]
        public void CanParseIsUsingBuiltInType()
        {
            var expression = "resource is Double";

            var parameters = new[]
            {
                Expression.Parameter(typeof(object), "resource"),
            };

            var expr = (Expression<Func<object, bool>>)DynamicExpression
                .ParseLambda(parameters, typeof(bool), expression);

            Console.WriteLine(expr);

            Assert.NotNull(expr);

            var func = expr.Compile();

            Assert.True(func(2.2));
        }
    }

    public enum MyEnum
    {
        Yes,
        No,
    }
}

