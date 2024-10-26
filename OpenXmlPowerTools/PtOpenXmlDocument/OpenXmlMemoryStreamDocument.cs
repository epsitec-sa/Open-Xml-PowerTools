using System;
using System.IO;
using System.IO.Packaging;
using System.Linq;
using System.Xml.Linq;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using JetBrains.Annotations;

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public sealed class OpenXmlMemoryStreamDocument : IDisposable
    {
        private readonly OpenXmlPowerToolsDocument _document;
        private MemoryStream _docMemoryStream;
        private Package _docPackage;

        public OpenXmlMemoryStreamDocument(OpenXmlPowerToolsDocument doc)
        {
            _document = doc;
            _docMemoryStream = new MemoryStream();
            _docMemoryStream.Write(doc.DocumentByteArray, 0, doc.DocumentByteArray.Length);

            try
            {
                _docPackage = Package.Open(_docMemoryStream, FileMode.Open);
            }
            catch (Exception e)
            {
                throw new PowerToolsDocumentException(e.Message);
            }
        }

        private OpenXmlMemoryStreamDocument(MemoryStream stream)
        {
            _docMemoryStream = stream;

            try
            {
                _docPackage = Package.Open(_docMemoryStream, FileMode.Open);
            }
            catch (Exception e)
            {
                throw new PowerToolsDocumentException(e.Message);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public static OpenXmlMemoryStreamDocument CreateWordprocessingDocument()
        {
            var stream = new MemoryStream();

            using (var doc = WordprocessingDocument.Create(stream, WordprocessingDocumentType.Document))
            {
                MainDocumentPart part = doc.AddMainDocumentPart();

                part.SetXElement(new XElement(W.document,
                    WmlDocument.NamespaceAttributes,
                    new XElement(W.body)));
            }

            return new OpenXmlMemoryStreamDocument(stream);
        }

        public static OpenXmlMemoryStreamDocument CreateSpreadsheetDocument()
        {
            var stream = new MemoryStream();

            using (var doc = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook))
            {
                WorkbookPart workbookPart = doc.AddWorkbookPart();

                XNamespace ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

                workbookPart.SetXElement(new XElement(ns + "workbook",
                    new XAttribute("xmlns", ns),
                    new XAttribute(XNamespace.Xmlns + "r", R.r),
                    new XElement(ns + "sheets")));

                doc.Close();
            }

            return new OpenXmlMemoryStreamDocument(stream);
        }

        public static OpenXmlMemoryStreamDocument CreatePresentationDocument()
        {
            var stream = new MemoryStream();

            using (var doc = PresentationDocument.Create(stream, PresentationDocumentType.Presentation))
            {
                PresentationPart presentationPart = doc.AddPresentationPart();

                XNamespace ns = "http://schemas.openxmlformats.org/presentationml/2006/main";
                XNamespace relationshipsns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
                XNamespace drawingns = "http://schemas.openxmlformats.org/drawingml/2006/main";

                presentationPart.SetXElement(new XElement(ns + "presentation",
                    new XAttribute(XNamespace.Xmlns + "a", drawingns),
                    new XAttribute(XNamespace.Xmlns + "r", relationshipsns),
                    new XAttribute(XNamespace.Xmlns + "p", ns),
                    new XElement(ns + "sldMasterIdLst"),
                    new XElement(ns + "sldIdLst"),
                    new XElement(ns + "notesSz", new XAttribute("cx", "6858000"), new XAttribute("cy", "9144000"))));
            }

            return new OpenXmlMemoryStreamDocument(stream);
        }

        public static OpenXmlMemoryStreamDocument CreatePackage()
        {
            var stream = new MemoryStream();
            Package package = Package.Open(stream, FileMode.Create);
            package.Close();
            return new OpenXmlMemoryStreamDocument(stream);
        }

        public Package GetPackage()
        {
            return _docPackage;
        }

        public WordprocessingDocument GetWordprocessingDocument()
        {
            try
            {
                if (GetDocumentType() != typeof(WordprocessingDocument))
                {
                    throw new PowerToolsDocumentException("Not a Wordprocessing document.");
                }

                return WordprocessingDocument.Open(_docPackage);
            }
            catch (Exception e)
            {
                throw new PowerToolsDocumentException(e.Message);
            }
        }

        public SpreadsheetDocument GetSpreadsheetDocument()
        {
            try
            {
                if (GetDocumentType() != typeof(SpreadsheetDocument))
                {
                    throw new PowerToolsDocumentException("Not a Spreadsheet document.");
                }

                return SpreadsheetDocument.Open(_docPackage);
            }
            catch (Exception e)
            {
                throw new PowerToolsDocumentException(e.Message);
            }
        }

        public PresentationDocument GetPresentationDocument()
        {
            try
            {
                if (GetDocumentType() != typeof(PresentationDocument))
                {
                    throw new PowerToolsDocumentException("Not a Presentation document.");
                }

                return PresentationDocument.Open(_docPackage);
            }
            catch (Exception e)
            {
                throw new PowerToolsDocumentException(e.Message);
            }
        }

        public Type GetDocumentType()
        {
            PackageRelationship relationship = _docPackage
                .GetRelationshipsByType("http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument")
                .FirstOrDefault();

            if (relationship == null)
            {
                relationship = _docPackage
                    .GetRelationshipsByType("http://purl.oclc.org/ooxml/officeDocument/relationships/officeDocument")
                    .FirstOrDefault();
            }

            if (relationship == null)
            {
                throw new PowerToolsDocumentException("Not an Open XML Document.");
            }

            PackagePart part = _docPackage.GetPart(PackUriHelper.ResolvePartUri(relationship.SourceUri, relationship.TargetUri));

            switch (part.ContentType)
            {
                case "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml":
                case "application/vnd.ms-word.document.macroEnabled.main+xml":
                case "application/vnd.ms-word.template.macroEnabledTemplate.main+xml":
                case "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml":
                    return typeof(WordprocessingDocument);

                case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml":
                case "application/vnd.ms-excel.sheet.macroEnabled.main+xml":
                case "application/vnd.ms-excel.template.macroEnabled.main+xml":
                case "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml":
                    return typeof(SpreadsheetDocument);

                case "application/vnd.openxmlformats-officedocument.presentationml.template.main+xml":
                case "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml":
                case "application/vnd.ms-powerpoint.template.macroEnabled.main+xml":
                case "application/vnd.ms-powerpoint.addin.macroEnabled.main+xml":
                case "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml":
                case "application/vnd.ms-powerpoint.presentation.macroEnabled.main+xml":
                    return typeof(PresentationDocument);
            }

            return null;
        }

        public OpenXmlPowerToolsDocument GetModifiedDocument()
        {
            _docPackage.Close();
            _docPackage = null;
            return new OpenXmlPowerToolsDocument(_document?.FileName, _docMemoryStream);
        }

        public WmlDocument GetModifiedWmlDocument()
        {
            _docPackage.Close();
            _docPackage = null;
            return new WmlDocument(_document?.FileName, _docMemoryStream);
        }

        public SmlDocument GetModifiedSmlDocument()
        {
            _docPackage.Close();
            _docPackage = null;
            return new SmlDocument(_document?.FileName, _docMemoryStream);
        }

        public PmlDocument GetModifiedPmlDocument()
        {
            _docPackage.Close();
            _docPackage = null;
            return new PmlDocument(_document?.FileName, _docMemoryStream);
        }

        public void Close()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _docPackage?.Close();
                _docMemoryStream?.Dispose();
            }

            if (_docPackage == null && _docMemoryStream == null)
            {
                return;
            }

            _docPackage = null;
            _docMemoryStream = null;

            GC.SuppressFinalize(this);
        }

        ~OpenXmlMemoryStreamDocument()
        {
            Dispose(false);
        }
    }
}
