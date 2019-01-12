# System.Linq.Dynamic
[![Build status](https://ci.appveyor.com/api/projects/status/f09il0lx2cdw3qpf?svg=true)](https://ci.appveyor.com/project/kdcllc/linq-dynamic)

Micorosft Dynamic Lynq class enabled for DotNet Core and .NET 4.5.

## Install
```cmd
    Install-Package Linq.Dynamic
    
    //unlisted due to Microsoft Nuget Policy
    Install-Package Microsoft.Linq.Dynamic -Version 1.0.3
```

[Dynamic LINQ (Part 1: Using the LINQ Dynamic Query Library) by ScottGu](https://weblogs.asp.net/scottgu/dynamic-linq-part-1-using-the-linq-dynamic-query-library)

An improvement of the original [Dynamic Linq MSDN sample](https://msdn.microsoft.com/en-us/vstudio/bb894665.aspx)

## Main Additions

* Allow to specify the list of types that can be used from within the expression.
* Improve enum handling, so that comparing enums will work.
* Add support for:
  - FirstOrDefault()
  - Fist()
  - SelectMany()
  - Distinct()
  - Union()
  - Concat()
  - Contains()
* Allow referencing the parent "it" context using "it_x" syntax.
* Add support for "as" and "is" operators.

## Docker-Compose support
Run tests in docker by
```
docker-compose up
```
## Special thanks to:
[Antoine Aubry](https://github.com/aaubry/DynamicQuery/)
