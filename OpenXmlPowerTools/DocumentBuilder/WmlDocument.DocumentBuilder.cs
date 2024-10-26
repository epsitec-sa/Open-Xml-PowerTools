using System.Collections.Generic;
using JetBrains.Annotations;

#nullable enable

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public sealed partial class WmlDocument
    {
        public IEnumerable<WmlDocument> SplitOnSections()
        {
            return DocumentBuilder.SplitOnSections(this);
        }
    }
}
