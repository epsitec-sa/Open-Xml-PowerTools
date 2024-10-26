using System;
using JetBrains.Annotations;

#nullable enable

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public sealed class PowerToolsDocumentException : Exception
    {
        public PowerToolsDocumentException(string message) : base(message)
        {
        }
    }
}
