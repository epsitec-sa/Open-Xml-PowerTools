using System.Collections.Generic;
using JetBrains.Annotations;

#nullable enable

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public sealed class DocumentBuilderSettings
    {
        public HashSet<string> CustomXmlGuidList { get; set; } = new();

        public bool NormalizeStyleIds { get; set; }
    }
}
