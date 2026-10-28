/**
 * Format a number with thousand separators
 * @param value - The number to format
 * @returns Formatted string (e.g., 1,234,567)
 */
export const formatNumber = (value: number): string => {
  if (typeof value !== "number" || isNaN(value)) return "0";

  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(value);
};

/**
 * Format a number with suffix (K, M, B)
 * @param value - The number to format
 * @returns Formatted string with suffix (e.g., 1.23M)
 */
export const formatNumberWithSuffix = (value: number): string => {
  if (typeof value !== "number" || isNaN(value)) return "0";

  const absValue = Math.abs(value);
  const sign = value < 0 ? "-" : "";

  if (absValue >= 1e9) {
    return `${sign}${(absValue / 1e9).toFixed(2)}B`;
  } else if (absValue >= 1e6) {
    return `${sign}${(absValue / 1e6).toFixed(2)}M`;
  } else if (absValue >= 1e3) {
    return `${sign}${(absValue / 1e3).toFixed(2)}K`;
  }

  return formatNumber(value);
};

/**
 * Shorten X-axis labels for better display
 * @param value - The label value
 * @param maxLength - Maximum length before truncation
 * @returns Shortened label
 */
export const shortenLabel = (value: string, maxLength: number = 15): string => {
  if (typeof value !== "string") return String(value);

  if (value.length <= maxLength) return value;

  return `${value.substring(0, maxLength)}...`;
};
