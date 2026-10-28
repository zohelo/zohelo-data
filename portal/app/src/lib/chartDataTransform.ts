/**
 * Advanced data transformation utilities for charting
 * Handles aggregations, grouping, sorting, and filtering
 */

import type { QueryResult } from "@/store";
import type { DataTransform, AggregationType, SeriesConfig } from "@/store";

type TransformedData = Record<string, unknown>[];

/**
 * Check if a column contains numeric values
 */
export const isNumericColumn = (data: Record<string, unknown>[], column: string): boolean => {
  if (data.length === 0) return false;

  // Sample first few non-null values
  const sampleSize = Math.min(10, data.length);
  for (let i = 0; i < sampleSize; i++) {
    const value = data[i][column];
    if (value !== null && value !== undefined) {
      return typeof value === "number" || !isNaN(Number(value));
    }
  }

  return false;
};

/**
 * Check if a column contains date/time values
 */
const isDateColumn = (data: Record<string, unknown>[], column: string): boolean => {
  if (data.length === 0) return false;

  const value = data[0][column];
  if (value instanceof Date) return true;
  if (typeof value === "string") {
    const date = new Date(value);
    return !isNaN(date.getTime());
  }

  return false;
};

/**
 * Aggregate values based on aggregation type
 */
const aggregate = (values: unknown[], aggregationType: AggregationType): number => {
  const numericValues = values.map((v) => Number(v)).filter((v) => !isNaN(v));

  if (numericValues.length === 0) return 0;

  switch (aggregationType) {
    case "sum":
      return numericValues.reduce((a, b) => a + b, 0);
    case "avg":
      return numericValues.reduce((a, b) => a + b, 0) / numericValues.length;
    case "count":
      return values.length;
    case "min":
      return Math.min(...numericValues);
    case "max":
      return Math.max(...numericValues);
    case "none":
    default:
      return numericValues[0] || 0;
  }
};

/**
 * Group data by a column and aggregate values
 */
const groupByColumn = (
  data: Record<string, unknown>[],
  groupByColumn: string,
  valueColumn: string,
  aggregationType: AggregationType
): TransformedData => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const grouped = new Map<string | number, any[]>();

  // Group values
  data.forEach((row) => {
    const key = row[groupByColumn] as string | number;
    if (key === null || key === undefined) return;

    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(row[valueColumn]);
  });

  // Aggregate grouped values
  const result: TransformedData = [];
  grouped.forEach((values, key) => {
    result.push({
      [groupByColumn]: key,
      [valueColumn]: aggregate(values, aggregationType),
    });
  });

  return result;
};

/**
 * Sort data by column
 */
const sortData = (
  data: TransformedData,
  sortBy: string,
  sortOrder: "asc" | "desc"
): TransformedData => {
  return [...data].sort((a, b) => {
    const aVal = a[sortBy];
    const bVal = b[sortBy];

    if (aVal === null || aVal === undefined) return 1;
    if (bVal === null || bVal === undefined) return -1;

    const comparison = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    return sortOrder === "asc" ? comparison : -comparison;
  });
};

/**
 * Limit data to top/bottom N rows
 */
const limitData = (data: TransformedData, limit: number): TransformedData => {
  return data.slice(0, Math.max(1, limit));
};

/**
 * Transform query result data based on configuration
 */
export const transformData = (
  result: QueryResult,
  transform?: DataTransform,
  xAxis?: string,
  yAxis?: string | SeriesConfig[]
): TransformedData => {
  let data = [...result.data];

  // Apply grouping and aggregation
  if (transform?.groupBy && transform.groupBy !== xAxis) {
    const valueColumn = typeof yAxis === "string" ? yAxis : yAxis?.[0]?.column;
    if (valueColumn) {
      const aggregation = transform.aggregation || "sum";
      data = groupByColumn(data, transform.groupBy, valueColumn, aggregation);
    }
  }

  // Apply sorting
  if (transform?.sortBy && transform.sortOrder && transform.sortOrder !== "none") {
    data = sortData(data, transform.sortBy, transform.sortOrder);
  }

  // Apply limit
  if (transform?.limit && transform.limit > 0) {
    data = limitData(data, transform.limit);
  }

  return data;
};

/**
 * Detect recommended chart types based on data characteristics
 * Only suggests chart types that are actually implemented in ChartVisualizationPro
 * Implemented types: bar, grouped_bar, stacked_bar, line, area, stacked_area, pie, donut, scatter
 */
export const suggestChartTypes = (
  result: QueryResult,
  xAxis?: string,
  yAxis?: string | SeriesConfig[]
): string[] => {
  if (!xAxis || !yAxis) return ["bar"];

  const suggestions: string[] = [];
  const yColumn = typeof yAxis === "string" ? yAxis : yAxis[0]?.column;

  if (!yColumn) return ["bar"];

  const hasMultipleSeries = Array.isArray(yAxis) && yAxis.length > 1;
  const xIsNumeric = isNumericColumn(result.data, xAxis);
  const xIsDate = isDateColumn(result.data, xAxis);
  const yIsNumeric = isNumericColumn(result.data, yColumn);
  const dataSize = result.data.length;

  // Time series data
  if (xIsDate && yIsNumeric) {
    suggestions.push("line", "area", "stacked_area");
  }

  // Categorical x-axis with numeric y
  if (!xIsNumeric && yIsNumeric) {
    suggestions.push("bar", "stacked_bar", "grouped_bar");
    if (dataSize <= 10) {
      suggestions.push("pie", "donut");
    }
  }

  // Numeric both axes
  if (xIsNumeric && yIsNumeric) {
    suggestions.push("scatter", "line");
  }

  // Multi-series
  if (hasMultipleSeries) {
    suggestions.push("grouped_bar", "stacked_bar");
  }

  return suggestions.length > 0 ? suggestions : ["bar"];
};
