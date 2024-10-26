using System;
using System.IO;
using System.IO.Packaging;
using System.Linq;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using JetBrains.Annotations;

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public class OpenXmlPowerToolsDocument
    {
        protected OpenXmlPowerToolsDocument(OpenXmlPowerToolsDocument original)
        {
            DocumentByteArray = new byte[original.DocumentByteArray.Length];
            Array.Copy(original.DocumentByteArray, DocumentByteArray, original.DocumentByteArray.Length);
            FileName = original.FileName;
        }

        protected OpenXmlPowerToolsDocument(OpenXmlPowerToolsDocument original, bool convertToTransitional)
        {
            if (convertToTransitional)
            {
                ConvertToTransitional(original.FileName, original.DocumentByteArray);
            }
            else
            {
                DocumentByteArray = new byte[original.DocumentByteArray.Length];
                Array.Copy(original.DocumentByteArray, DocumentByteArray, original.DocumentByteArray.Length);
                FileName = original.FileName;
            }
        }

        protected OpenXmlPowerToolsDocument(string fileName)
        {
            FileName = fileName;
            DocumentByteArray = File.ReadAllBytes(fileName);
        }

        protected OpenXmlPowerToolsDocument(string fileName, bool convertToTransitional)
        {
            FileName = fileName;

            if (convertToTransitional)
            {
                byte[] tempByteArray = File.ReadAllBytes(fileName);
                ConvertToTransitional(fileName, tempByteArray);
            }
            else
            {
                FileName = fileName;
                DocumentByteArray = File.ReadAllBytes(fileName);
            }
        }

        protected OpenXmlPowerToolsDocument(byte[] byteArray)
        {
            DocumentByteArray = new byte[byteArray.Length];
            Array.Copy(byteArray, DocumentByteArray, byteArray.Length);
            FileName = null;
        }

        protected OpenXmlPowerToolsDocument(byte[] byteArray, bool convertToTransitional)
        {
            if (convertToTransitional)
            {
                ConvertToTransitional(null, byteArray);
            }
            else
            {
                DocumentByteArray = new byte[byteArray.Length];
                Array.Copy(byteArray, DocumentByteArray, byteArray.Length);
                FileName = null;
            }
        }

        protected OpenXmlPowerToolsDocument(string fileName, MemoryStream memStream, bool convertToTransitional)
        {
            if (convertToTransitional)
            {
                ConvertToTransitional(fileName, memStream.ToArray());
            }
            else
            {
                FileName = fileName;
                DocumentByteArray = new byte[memStream.Length];
                Array.Copy(memStream.GetBuffer(), DocumentByteArray, memStream.Length);
            }
        }

        internal OpenXmlPowerToolsDocument(string fileName, MemoryStream memStream)
        {
            FileName = fileName;
            DocumentByteArray = new byte[memStream.Length];
            Array.Copy(memStream.GetBuffer(), DocumentByteArray, memStream.Length);
        }

        public string FileName { get; set; }

        public byte[] DocumentByteArray { get; set; }

        public static OpenXmlPowerToolsDocument FromFileName(string fileName)
        {
            byte[] bytes = File.ReadAllBytes(fileName);
            Type type;

            try
            {
                type = GetDocumentType(bytes);
            }
            catch (FileFormatException)
            {
                throw new PowerToolsDocumentException("Not an Open XML document.");
            }

            if (type == typeof(WordprocessingDocument))
            {
                return new WmlDocument(fileName, bytes);
            }

            if (type == typeof(SpreadsheetDocument))
            {
                return new SmlDocument(fileName, bytes);
            }

            if (type == typeof(PresentationDocument))
            {
                return new PmlDocument(fileName, bytes);
            }

            if (type == typeof(Package))
            {
                return new OpenXmlPowerToolsDocument(bytes) { FileName = fileName };
            }

            throw new PowerToolsDocumentException("Not an Open XML document.");
        }

        public static OpenXmlPowerToolsDocument FromDocument(OpenXmlPowerToolsDocument doc)
        {
            Type type = doc.GetDocumentType();

            if (type == typeof(WordprocessingDocument))
            {
                return new WmlDocument(doc);
            }

            if (type == typeof(SpreadsheetDocument))
            {
                return new SmlDocument(doc);
            }

            if (type == typeof(PresentationDocument))
            {
                return new PmlDocument(doc);
            }

            return null; // This should not be possible from a valid OpenXmlPowerToolsDocument object
        }

        private void ConvertToTransitional(string fileName, byte[] tempByteArray)
        {
            Type type;

            try
            {
                type = GetDocumentType(tempByteArray);
            }
            catch (FileFormatException)
            {
                throw new PowerToolsDocumentException("Not an Open XML document.");
            }

            using var ms = new MemoryStream();
            ms.Write(tempByteArray, 0, tempByteArray.Length);

            if (type == typeof(WordprocessingDocument))
            {
                using WordprocessingDocument sDoc = WordprocessingDocument.Open(ms, true);

                // The following code forces the SDK to serialize
                foreach (IdPartPair part in sDoc.Parts)
                {
                    try
                    {
                        OpenXmlPartRootElement unused = part.OpenXmlPart.RootElement;
                    }
                    catch (Exception)
                    {
                        // Ignore
                    }
                }
            }
            else if (type == typeof(SpreadsheetDocument))
            {
                using SpreadsheetDocument sDoc = SpreadsheetDocument.Open(ms, true);

                // The following code forces the SDK to serialize
                foreach (IdPartPair part in sDoc.Parts)
                {
                    try
                    {
                        OpenXmlPartRootElement unused = part.OpenXmlPart.RootElement;
                    }
                    catch (Exception)
                    {
                        // Ignore
                    }
                }
            }
            else if (type == typeof(PresentationDocument))
            {
                using PresentationDocument sDoc = PresentationDocument.Open(ms, true);

                // The following code forces the SDK to serialize
                foreach (IdPartPair part in sDoc.Parts)
                {
                    try
                    {
                        OpenXmlPartRootElement unused = part.OpenXmlPart.RootElement;
                    }
                    catch (Exception)
                    {
                        // Ignore
                    }
                }
            }

            FileName = fileName;
            DocumentByteArray = ms.ToArray();
        }

        public string GetName()
        {
            if (FileName == null)
            {
                return "Unnamed Document";
            }

            var file = new FileInfo(FileName);
            return file.Name;
        }

        public void SaveAs(string fileName)
        {
            File.WriteAllBytes(fileName, DocumentByteArray);
        }

        public void Save()
        {
            if (FileName == null)
            {
                throw new InvalidOperationException("Attempting to Save a document that has no file name.  Use SaveAs instead.");
            }

            File.WriteAllBytes(FileName, DocumentByteArray);
        }

        public MemoryStream ToMemoryStream()
        {
            var stream = new MemoryStream();
            WriteByteArray(stream);
            stream.Seek(0, SeekOrigin.Begin);

            return stream;
        }

        public void WriteByteArray(Stream stream)
        {
            stream.Write(DocumentByteArray, 0, DocumentByteArray.Length);
        }

        public Type GetDocumentType()
        {
            return GetDocumentType(DocumentByteArray);
        }

        private static Type GetDocumentType(byte[] bytes)
        {
            // Relationship types:
            const string coreDocument = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
            const string strictCoreDocument = "http://purl.oclc.org/ooxml/officeDocument/relationships/officeDocument";

            using var stream = new MemoryStream();

            stream.Write(bytes, 0, bytes.Length);

            using Package package = Package.Open(stream, FileMode.Open);

            PackageRelationship relationship =
                package.GetRelationshipsByType(coreDocument).FirstOrDefault() ??
                package.GetRelationshipsByType(strictCoreDocument).FirstOrDefault();

            if (relationship == null)
            {
                return null;
            }

            Uri partUri = PackUriHelper.ResolvePartUri(relationship.SourceUri, relationship.TargetUri);
            PackagePart part = package.GetPart(partUri);

            return part.ContentType switch
            {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml" =>
                    typeof(WordprocessingDocument),

                "application/vnd.ms-word.document.macroEnabled.main+xml" => typeof(WordprocessingDocument),
                "application/vnd.ms-word.template.macroEnabledTemplate.main+xml" => typeof(WordprocessingDocument),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml" =>
                    typeof(WordprocessingDocument),

                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml" => typeof(SpreadsheetDocument),
                "application/vnd.ms-excel.sheet.macroEnabled.main+xml" => typeof(SpreadsheetDocument),
                "application/vnd.ms-excel.template.macroEnabled.main+xml" => typeof(SpreadsheetDocument),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml" => typeof(SpreadsheetDocument),
                "application/vnd.openxmlformats-officedocument.presentationml.template.main+xml" => typeof(PresentationDocument),
                "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml" =>
                    typeof(PresentationDocument),

                "application/vnd.ms-powerpoint.template.macroEnabled.main+xml" => typeof(PresentationDocument),
                "application/vnd.ms-powerpoint.addin.macroEnabled.main+xml" => typeof(PresentationDocument),
                "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml" => typeof(PresentationDocument),
                "application/vnd.ms-powerpoint.presentation.macroEnabled.main+xml" => typeof(PresentationDocument),
                _ => typeof(Package),
            };
        }
    }
}
