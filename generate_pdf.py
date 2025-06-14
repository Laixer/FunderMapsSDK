#!/usr/bin/env python3

import os
import time
import asyncio
import argparse
from pathlib import Path

from fundermapssdk.command import FunderMapsCommand


class PDFGenerateCommand(FunderMapsCommand):
    """Command to generate PDFs from URLs using the FunderMaps SDK."""

    def __init__(self):
        super().__init__(description="Generate PDFs from URLs using PDF.co service")

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument("url", type=str, help="URL to convert to PDF")
        parser.add_argument(
            "--output-name",
            type=str,
            help="Output filename for the PDF (without extension)",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default="./pdfs",
            help="Output directory for generated PDFs (default: ./pdfs)",
        )

    def _generate_output_name(self, url: str) -> str:
        """Generate a default output name from URL if not provided."""
        if hasattr(self.args, "output_name") and self.args.output_name:
            return self.args.output_name

        # Extract domain and path from URL for a meaningful name
        from urllib.parse import urlparse

        parsed = urlparse(url)
        path = parsed.path.replace("/", "_").strip("_")

        if path:
            return path

    def _ensure_output_directory(self) -> Path:
        """Ensure output directory exists and return Path object."""
        output_dir = Path(self.args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    async def _generate_single_pdf(self, url: str, output_name: str) -> bool:
        """Generate a single PDF from URL."""
        self.logger.info("Starting PDF generation")
        start_time = time.time()

        try:
            # Generate PDF using the SDK
            result = await self.fundermaps.pdf.generate_pdf(url, output_name)

            if result.get("error"):
                self.logger.error(
                    f"PDF generation failed: {result.get('message', 'Unknown error')}"
                )
                return False

            # The PDF.co service returns the PDF URL in the response
            if "url" in result:
                pdf_url = result["url"]
                self.logger.info("PDF generated successfully")

                # Optionally download the PDF to local directory
                if hasattr(self.args, "output_dir") and self.args.output_dir:
                    await self._download_pdf(pdf_url, output_name)
                    self._upload_pdf(output_name)

            elapsed = time.time() - start_time
            self.logger.info(f"PDF generation completed in {elapsed:.2f}s")
            return True

        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(
                f"PDF generation failed after {elapsed:.2f}s: {e}", exc_info=True
            )
            return False

    async def _download_pdf(self, pdf_url: str, output_name: str) -> bool:
        """Download the generated PDF to local directory."""
        import httpx

        try:
            output_dir = self._ensure_output_directory()
            output_path = output_dir / f"{output_name}.pdf"

            self.logger.info(f"Downloading PDF to: {output_path}")

            async with httpx.AsyncClient() as client:
                response = await client.get(pdf_url)
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    f.write(response.content)

                self.logger.info(f"PDF downloaded successfully: {output_path}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to download PDF: {e}", exc_info=True)
            return False

    def _upload_pdf(self, output_name: str) -> bool:
        try:
            self.logger.info(f"Uploading {output_name}.pdf to S3")

            with self.fundermaps.s3 as s3:
                s3_path = f"artifacts/report-pdf/{output_name}.pdf"
                s3.upload_file(
                    os.path.join("pdfs", f"{output_name}.pdf"),
                    s3_path,
                    bucket="fundermaps",
                )
            return True

        except Exception as e:
            self.logger.error(f"Failed to upload PDF to S3: {e}", exc_info=True)
            return False

    async def execute(self) -> int:
        """Execute the PDF generation command."""
        try:
            # Validate URL
            from urllib.parse import urlparse

            parsed = urlparse(self.args.url)
            if not parsed.scheme or not parsed.netloc:
                self.logger.error(f"Invalid URL provided: {self.args.url}")
                return 1

            # Generate output name
            output_name = self._generate_output_name(self.args.url)
            self.logger.info(f"Output name: {output_name}")

            # Generate PDF
            success = await self._generate_single_pdf(self.args.url, output_name)

            if success:
                self.logger.info("PDF generation completed successfully")
                return 0
            else:
                self.logger.error("PDF generation failed")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(PDFGenerateCommand().run())
    exit(exit_code)
