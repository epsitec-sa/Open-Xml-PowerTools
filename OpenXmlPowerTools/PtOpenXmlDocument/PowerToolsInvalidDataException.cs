using System;

#nullable enable

namespace OpenXmlPowerTools
{
    internal sealed class PowerToolsInvalidDataException : Exception
    {
        public PowerToolsInvalidDataException(string message) : base(message)
        {
        }
    }
}
