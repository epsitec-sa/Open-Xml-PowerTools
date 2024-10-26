using System;

namespace OpenXmlPowerTools
{
    public sealed class DocumentBuilderException : OpenXmlPowerToolsException
    {
        public DocumentBuilderException(string message)
            : base(message)
        {
        }

        public DocumentBuilderException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
