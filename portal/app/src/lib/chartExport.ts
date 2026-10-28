/**
 * Chart export utilities for PNG, SVG formats
 * Uses html2canvas and SVG manipulation for high-quality exports
 */

/**
 * Helper to trigger a blob download
 */
const downloadBlob = (blob: Blob, fileName: string) => {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};

/**
 * Export chart as PNG image.
 * For canvas-based charts (uPlot), captures the canvas directly.
 * For SVG-based charts (pie/donut), falls back to html2canvas.
 */
export const exportChartAsPNG = async (
  chartElement: HTMLElement,
  fileName: string = "chart.png",
  backgroundColor: string = "#ffffff"
): Promise<void> => {
  try {
    // Direct canvas capture (uPlot renders to <canvas>)
    const sourceCanvas = chartElement.querySelector("canvas");
    if (sourceCanvas && sourceCanvas.width > 0 && sourceCanvas.height > 0) {
      const offscreen = document.createElement("canvas");
      offscreen.width = sourceCanvas.width;
      offscreen.height = sourceCanvas.height;
      const ctx = offscreen.getContext("2d");
      if (!ctx) throw new Error("Failed to get canvas context");
      ctx.fillStyle = backgroundColor;
      ctx.fillRect(0, 0, offscreen.width, offscreen.height);
      ctx.drawImage(sourceCanvas, 0, 0);

      const blob = await new Promise<Blob>((resolve, reject) => {
        offscreen.toBlob(
          (b) => (b ? resolve(b) : reject(new Error("Failed to create blob"))),
          "image/png"
        );
      });
      downloadBlob(blob, fileName);
      return;
    }

    // Fallback: html2canvas for SVG-based charts (pie/donut)
    const html2canvas = (await import("html2canvas")).default;
    const rendered = await html2canvas(chartElement, {
      backgroundColor,
      scale: 2,
      logging: false,
      useCORS: true,
    });

    rendered.toBlob((blob) => {
      if (!blob) throw new Error("Failed to create image blob");
      downloadBlob(blob, fileName);
    }, "image/png");
  } catch (error) {
    console.error("Failed to export chart as PNG:", error);
    throw new Error("Failed to export chart as PNG. Please try again.");
  }
};
