# Poster

Draft poster for COMP-SCI 5540 (`HyperLogistics_Stream_Poster.pdf`, one page, 48 x 36 inches landscape, as Hack-A-Roo requires).

Every chart is built from measured results in the repo, so rebuild after any new run:

```bash
python docs/poster/build_figures.py --url https://github.com/<owner>/<repo>   # charts + QR code
```

Render `poster.html` to PDF and PNG with Chrome or Edge (headless):

```bash
chrome --headless=new --no-pdf-header-footer --print-to-pdf=docs/poster/HyperLogistics_Stream_Poster.pdf docs/poster/poster.html
python docs/poster/resize_pdf.py docs/poster/HyperLogistics_Stream_Poster.pdf docs/poster/_big.pdf   # scale to 48 x 36 in, then replace the PDF with _big.pdf
chrome --headless=new --hide-scrollbars --window-size=2000,1500 --screenshot=docs/poster/poster_preview.png docs/poster/poster.html
```

Numbers typed into `poster.html` (10.53 MAE, 17%, +1.0%, 4,448 records/s, 60+ tests, 77% / 12.2 / 9.7 live-vs-history) come from
`EVALUATION.md` and `models/live_vs_historical.*`. Update them there and here if results change (the benchmark is re-run on Nov 2).
