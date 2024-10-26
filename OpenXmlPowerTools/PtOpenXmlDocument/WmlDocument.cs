using System.Collections.Generic;
using System.IO;
using System.IO.Packaging;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using DocumentFormat.OpenXml.Packaging;

namespace OpenXmlPowerTools
{
    /*
Here is modification of a WmlDocument:
    public static WmlDocument SimplifyMarkup(WmlDocument doc, SimplifyMarkupSettings settings)
    {
        using (OpenXmlMemoryStreamDocument streamDoc = new OpenXmlMemoryStreamDocument(doc))
        {
            using (WordprocessingDocument document = streamDoc.GetWordprocessingDocument())
            {
                SimplifyMarkup(document, settings);
            }
            return streamDoc.GetModifiedWmlDocument();
        }
    }

Here is read-only of a WmlDocument:

    public static string GetBackgroundColor(WmlDocument doc)
    {
        using (OpenXmlMemoryStreamDocument streamDoc = new OpenXmlMemoryStreamDocument(doc))
        using (WordprocessingDocument document = streamDoc.GetWordprocessingDocument())
        {
            XDocument mainDocument = document.MainDocumentPart.GetXDocument();
            XElement backgroundElement = mainDocument.Descendants(W.background).FirstOrDefault();
            return (backgroundElement == null) ? string.Empty : backgroundElement.Attribute(W.color).Value;
        }
    }

Here is creating a new WmlDocument:

    private OpenXmlPowerToolsDocument CreateSplitDocument(WordprocessingDocument source, List<XElement> contents, string newFileName)
    {
        using (OpenXmlMemoryStreamDocument streamDoc = OpenXmlMemoryStreamDocument.CreateWordprocessingDocument())
        {
            using (WordprocessingDocument document = streamDoc.GetWordprocessingDocument())
            {
                DocumentBuilder.FixRanges(source.MainDocumentPart.GetXDocument(), contents);
                PowerToolsExtensions.SetContent(document, contents);
            }
            OpenXmlPowerToolsDocument newDoc = streamDoc.GetModifiedDocument();
            newDoc.FileName = newFileName;
            return newDoc;
        }
    }
*/
    public partial class WmlDocument : OpenXmlPowerToolsDocument
    {
        public static readonly IReadOnlyCollection<XAttribute> NamespaceAttributes = new List<XAttribute>
        {
            new(XNamespace.Xmlns + "wpc", WPC.wpc),
            new(XNamespace.Xmlns + "mc", MC.mc),
            new(XNamespace.Xmlns + "o", O.o),
            new(XNamespace.Xmlns + "r", R.r),
            new(XNamespace.Xmlns + "m", M.m),
            new(XNamespace.Xmlns + "v", VML.vml),
            new(XNamespace.Xmlns + "wp14", WP14.wp14),
            new(XNamespace.Xmlns + "wp", WP.wp),
            new(XNamespace.Xmlns + "w10", W10.w10),
            new(XNamespace.Xmlns + "w", W.w),
            new(XNamespace.Xmlns + "w14", W14.w14),
            new(XNamespace.Xmlns + "wpg", WPG.wpg),
            new(XNamespace.Xmlns + "wpi", WPI.wpi),
            new(XNamespace.Xmlns + "wne", WNE.wne),
            new(XNamespace.Xmlns + "wps", WPS.wps),
            new(MC.Ignorable, "w14 wp14"),
        };

        public WmlDocument(OpenXmlPowerToolsDocument original)
            : base(original)
        {
            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(OpenXmlPowerToolsDocument original, bool convertToTransitional)
            : base(original, convertToTransitional)
        {
            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(string fileName)
            : base(fileName)
        {
            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(string fileName, bool convertToTransitional)
            : base(fileName, convertToTransitional)
        {
            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(string fileName, byte[] byteArray)
            : base(byteArray)
        {
            FileName = fileName;

            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(string fileName, byte[] byteArray, bool convertToTransitional)
            : base(byteArray, convertToTransitional)
        {
            FileName = fileName;

            if (GetDocumentType() != typeof(WordprocessingDocument))
            {
                throw new PowerToolsDocumentException("Not a Wordprocessing document.");
            }
        }

        public WmlDocument(string fileName, MemoryStream memStream)
            : base(fileName, memStream)
        {
        }

        public WmlDocument(string fileName, MemoryStream memStream, bool convertToTransitional)
            : base(fileName, memStream, convertToTransitional)
        {
        }

        public WmlDocument(WmlDocument other, params XElement[] replacementParts)
            : base(other)
        {
            using var streamDoc = new OpenXmlMemoryStreamDocument(this);

            using (Package package = streamDoc.GetPackage())
            {
                foreach (XElement replacementPart in replacementParts)
                {
                    XAttribute uriAttribute = replacementPart.Attribute(PtOpenXml.Uri);

                    if (uriAttribute == null)
                    {
                        throw new OpenXmlPowerToolsException("Replacement part does not contain a Uri as an attribute");
                    }

                    string uri = uriAttribute.Value;
                    PackagePart part = package.GetParts().First(p => p.Uri.ToString() == uri);

                    using Stream partStream = part.GetStream(FileMode.Create, FileAccess.Write);
                    using var partXmlWriter = XmlWriter.Create(partStream);
                    replacementPart.Save(partXmlWriter);
                }
            }

            DocumentByteArray = streamDoc.GetModifiedDocument().DocumentByteArray;
        }

        public XElement MainDocumentPart
        {
            get
            {
                using var ms = new MemoryStream(DocumentByteArray);
                using WordprocessingDocument wDoc = WordprocessingDocument.Open(ms, false);

                return wDoc.MainDocumentPart.GetXElementOrThrow();
            }
        }
    }
}
