using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using Microsoft.SqlServer.Server;
using Microsoft.SqlServer.TransactSql.ScriptDom;


namespace CLRF_ParseView
{
    public class UserDefinedFunctions
    {
        private class ViewColumn
        {
            public SqlString Alias;
            public SqlString TableColumnName;
            public SqlString TableAlias;
            public SqlString TableName;
        }
        

        [SqlFunction(
            FillRowMethodName = "FindColumns", 
            TableDefinition = "Alias nvarchar(500), TableColumnName nvarchar(500), TableAlias nvarchar(500), TableName nvarchar(500)")]
        public static IEnumerable ParseView(string tsql)
        {
            LinkedList<ViewColumn> viewColumns = new LinkedList<ViewColumn>();
            viewColumns.AddLast(new ViewColumn()
            {
                Alias = new SqlString("Alias"),
                TableName = new SqlString("TableName"),
                TableAlias = new SqlString("TableAlias"),
                TableColumnName = new SqlString("TableColumnName")
            });

            return viewColumns;
        }


        public static void FindColumns(object objColumns, out SqlString alias, out SqlString tableColumnName, out SqlString tableAlias, out SqlString tableName)
        {
            ViewColumn viewColumn = (ViewColumn)objColumns;
            alias = viewColumn.Alias;
            tableColumnName = viewColumn.TableColumnName;
            tableAlias = viewColumn.TableAlias;
            tableName = viewColumn.TableName;
        }


        private static IEnumerable<ViewColumn> ParseViewStatementBody(ViewStatementBody sqlStatement)
        {
            LinkedList<ViewColumn> result = new LinkedList<ViewColumn>();
            SelectStatement aSelectStatement = sqlStatement.SelectStatement;
            QueryExpression aQueryExpression = aSelectStatement.QueryExpression;

            if (aQueryExpression.GetType() != typeof(QuerySpecification))
            {
                return null;
            }

            QuerySpecification aQuerySpecification = (QuerySpecification)aQueryExpression;
            var selectElements = aQuerySpecification.SelectElements
                .OfType<SelectScalarExpression>()
                .ToList();

            foreach (SelectScalarExpression aSelectScalarExpression in selectElements)
            {
                string identStr = string.Empty;
                IdentifierOrValueExpression aIdentifierOrValueExpression =
                    aSelectScalarExpression.ColumnName;
                if (aIdentifierOrValueExpression != null)
                {
                    if (aIdentifierOrValueExpression.ValueExpression == null)
                    {
                        identStr = aIdentifierOrValueExpression.Identifier.Value;
                    }
                }

                result.AddLast(new ViewColumn());
                result.Last.Value.Alias = identStr;

                ScalarExpression aScalarExpression = aSelectScalarExpression.Expression;
                var aMultiPartIdentifier = FindSelectScalarExperssionRecurse(aScalarExpression);
                string aColumnInfoTableColumnName = "";
                string aColumnInfoTableAlias = "";
                string aColumnInfoReferencedTableName = "";

                if (aMultiPartIdentifier != null)
                {
                    int aIdentIdx = 0;
                    foreach (var aIdentifier in aMultiPartIdentifier.Identifiers)
                    {
                        if (aMultiPartIdentifier.Identifiers.Count == 2)
                        {
                            if (aIdentIdx == 0)
                            {
                                aColumnInfoTableAlias = aIdentifier.Value;
                            }
                            if (aIdentIdx == 1)
                                aColumnInfoTableColumnName = aIdentifier.Value;
                        }

                        if (aMultiPartIdentifier.Identifiers.Count == 3)
                        {
                            if (aIdentIdx == 1)
                            {
                                aColumnInfoReferencedTableName = aIdentifier.Value;
                            }
                            if (aIdentIdx == 2)
                                aColumnInfoTableColumnName = aIdentifier.Value;
                        }

                        result.Last.Value.TableAlias = aColumnInfoTableAlias;
                        result.Last.Value.TableColumnName = aColumnInfoTableColumnName;
                        result.Last.Value.TableName = aColumnInfoReferencedTableName;

                        aIdentIdx = aIdentIdx + 1;
                    }
                }
            }
            return result;
        }

        private static MultiPartIdentifier FindSelectScalarExperssionRecurse(ScalarExpression aScalarExpression)
        {
            if (aScalarExpression.GetType() == typeof(ColumnReferenceExpression))
            {
                ColumnReferenceExpression aColumnReferenceExpression = (ColumnReferenceExpression)aScalarExpression;
                MultiPartIdentifier aMultiPartIdentifier = aColumnReferenceExpression.MultiPartIdentifier;
                return aMultiPartIdentifier;
            }

            if (aScalarExpression.GetType() == typeof(ConvertCall))
            {
                ConvertCall aConvertCall = (ConvertCall)aScalarExpression;
                ScalarExpression aScalarExpressionParameter = aConvertCall.Parameter;
                FindSelectScalarExperssionRecurse(aScalarExpressionParameter);
            }

            return null;
        }
    }
}
