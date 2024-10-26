using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Xml.Linq;

#nullable enable

namespace OpenXmlPowerTools
{
    public static partial class DocumentBuilder
    {
        private const string Yes = "yes";
        private const string Utf8 = "UTF-8";
        private const string OnePointZero = "1.0";

        private static readonly IReadOnlyCollection<XAttribute> NamespaceAttributes = WmlDocument.NamespaceAttributes;

        private static Dictionary<XName, XName[]>? _relationshipMarkup;

        private static Dictionary<XName, XName[]> RelationshipMarkup =>
            _relationshipMarkup ??= new Dictionary<XName, XName[]>
            {
                //{ button,           new [] { image }},
                { A.blip, new[] { R.embed, R.link } },
                { A.hlinkClick, new[] { R.id } },
                { A.relIds, new[] { R.cs, R.dm, R.lo, R.qs } },
                //{ a14:imgLayer,     new [] { R.embed }},
                //{ ax:ocx,           new [] { R.id }},
                { C.chart, new[] { R.id } },
                { C.externalData, new[] { R.id } },
                { C.userShapes, new[] { R.id } },
                { DGM.relIds, new[] { R.cs, R.dm, R.lo, R.qs } },
                { O.OLEObject, new[] { R.id } },
                { VML.fill, new[] { R.id } },
                { VML.imagedata, new[] { R.href, R.id, R.pict } },
                { VML.stroke, new[] { R.id } },
                { W.altChunk, new[] { R.id } },
                { W.attachedTemplate, new[] { R.id } },
                { W.control, new[] { R.id } },
                { W.dataSource, new[] { R.id } },
                { W.embedBold, new[] { R.id } },
                { W.embedBoldItalic, new[] { R.id } },
                { W.embedItalic, new[] { R.id } },
                { W.embedRegular, new[] { R.id } },
                { W.footerReference, new[] { R.id } },
                { W.headerReference, new[] { R.id } },
                { W.headerSource, new[] { R.id } },
                { W.hyperlink, new[] { R.id } },
                { W.printerSettings, new[] { R.id } },
                { W.recipientData, new[] { R.id } }, // Mail merge, not required
                { W.saveThroughXslt, new[] { R.id } },
                { W.sourceFileName, new[] { R.id } }, // Framesets, not required
                { W.src, new[] { R.id } }, // Mail merge, not required
                { W.subDoc, new[] { R.id } }, // Sub documents, not required
                //{ w14:contentPart,  new [] { R.id }},
                { WNE.toolbarData, new[] { R.id } },
            };

        private sealed class Atbi
        {
            public XElement? BlockLevelContent;
            public int Index;
        }

        private sealed class Atbid
        {
            public XElement? BlockLevelContent;
            public int Index;
            public int Div;
        }

        private sealed class CachedHeaderFooter
        {
            public XName Ref { get; set; } = null!;

            public string Type { get; set; } = null!;

            [DisallowNull]
            public string? CachedPartRid { get; set; }
        }

        private sealed class ReplaceSemaphore
        {
        }
    }
}
