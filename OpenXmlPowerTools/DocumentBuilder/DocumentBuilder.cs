#define TestForUnsupportedDocuments
#define MergeStylesWithSameNames

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using JetBrains.Annotations;

#nullable enable

namespace OpenXmlPowerTools
{
    [PublicAPI]
    public static partial class DocumentBuilder
    {
        public static void BuildDocument(List<Source> sources, string fileName)
        {
            BuildDocument(sources, fileName, new DocumentBuilderSettings());
        }

        public static void BuildDocument(List<Source> sources, string fileName, DocumentBuilderSettings settings)
        {
            using var streamDoc = OpenXmlMemoryStreamDocument.CreateWordprocessingDocument();

            using (WordprocessingDocument output = streamDoc.GetWordprocessingDocument())
            {
                BuildDocument(sources, output, settings);
                output.Close();
            }

            streamDoc.GetModifiedDocument().SaveAs(fileName);
        }

        public static WmlDocument BuildDocument(List<Source> sources)
        {
            return BuildDocument(sources, new DocumentBuilderSettings());
        }

        public static WmlDocument BuildDocument(List<Source> sources, DocumentBuilderSettings settings)
        {
            using var streamDoc = OpenXmlMemoryStreamDocument.CreateWordprocessingDocument();

            using (WordprocessingDocument output = streamDoc.GetWordprocessingDocument())
            {
                BuildDocument(sources, output, settings);
                output.Close();
            }

            return streamDoc.GetModifiedWmlDocument();
        }

        private static void BuildDocument(List<Source> sources, WordprocessingDocument output, DocumentBuilderSettings settings)
        {
            // At this point, the list of sources might be empty. The output WordprocessingDocument is
            // a new document with a minimal MainDocumentPart.

            if (!sources.Any())
            {
                return;
            }

            // Make sure that, for a given style name, the same style ID is used for all documents.
            if (settings is { NormalizeStyleIds: true })
            {
                sources = NormalizeStyleNamesAndIds(sources);
            }

            // This list is used to eliminate duplicate images
            var images = new List<ImageData>();

            using (var streamDoc = new OpenXmlMemoryStreamDocument(sources[0].WmlDocument))
            {
                using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();

                CopyStartingParts(doc, output, images);
                CopySpecifiedCustomXmlParts(doc, output, settings);
            }

            var sourceNum2 = 0;

            foreach (Source source in sources)
            {
                if (source.InsertId != null)
                {
                    while (true)
                    {
                        // Modify AppendDocument so that it can take a part.
                        // for each in main document part, header parts, footer parts
                        // are there any PtOpenXml.Insert elements in any of them?
                        // if so, then open and process all.
                        bool foundInMainDocPart = output
                            .MainDocumentPart!
                            .GetXDocument()
                            .Descendants(PtOpenXml.Insert)
                            .Any(d => (string) d.Attribute(PtOpenXml.Id) == source.InsertId);

                        if (!foundInMainDocPart)
                        {
                            break;
                        }

                        using var streamDoc = new OpenXmlMemoryStreamDocument(source.WmlDocument);
                        using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();
                        doc.ThrowIfNotValid();

                        // Throws exceptions if a document contains unsupported content
                        TestForUnsupportedDocument(doc, sources.IndexOf(source));

                        if (source.KeepSections && source.DiscardHeadersAndFootersInKeptSections)
                        {
                            RemoveHeadersAndFootersFromSections(doc);
                        }
                        else if (source.KeepSections)
                        {
                            ProcessSectionsForLinkToPreviousHeadersAndFooters(doc);
                        }

                        List<XElement> contents = doc
                            .MainDocumentPart
                            .GetXElementOrThrow()
                            .Elements(W.body)
                            .Elements()
                            .Skip(source.Start)
                            .Take(source.Count)
                            .ToList();

                        try
                        {
                            AppendDocument(doc, output, contents, source.KeepSections, source.InsertId,
                                images);
                        }
                        catch (DocumentBuilderInternalException dbie)
                        {
                            if (dbie.Message.Contains("{0}"))
                            {
                                throw new DocumentBuilderException(string.Format(dbie.Message, sourceNum2));
                            }

                            throw;
                        }
                    }
                }
                else
                {
                    using var streamDoc = new OpenXmlMemoryStreamDocument(source.WmlDocument);
                    using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();
                    doc.ThrowIfNotValid();

                    // Throws exceptions if a document contains unsupported content
                    TestForUnsupportedDocument(doc, sources.IndexOf(source));

                    if (source.KeepSections && source.DiscardHeadersAndFootersInKeptSections)
                    {
                        RemoveHeadersAndFootersFromSections(doc);
                    }
                    else if (source.KeepSections)
                    {
                        ProcessSectionsForLinkToPreviousHeadersAndFooters(doc);
                    }

                    List<XElement> contents = doc
                        .MainDocumentPart
                        .GetXElementOrThrow()
                        .Elements(W.body)
                        .Elements()
                        .Skip(source.Start)
                        .Take(source.Count)
                        .ToList();

                    try
                    {
                        AppendDocument(doc, output, contents, source.KeepSections, null, images);
                    }
                    catch (DocumentBuilderInternalException dbie)
                    {
                        if (dbie.Message.Contains("{0}"))
                        {
                            throw new DocumentBuilderException(string.Format(dbie.Message, sourceNum2));
                        }

                        throw;
                    }
                }

                ++sourceNum2;
            }

            if (!sources.Any(s => s.KeepSections))
            {
                using var streamDoc = new OpenXmlMemoryStreamDocument(sources[0].WmlDocument);
                using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();

                XElement? body = doc.MainDocumentPart?.GetXElement()?.Element(W.body);

                if (body != null && body.Elements().Any())
                {
                    XElement? sectPr = body.Elements().LastOrDefault();

                    if (sectPr != null && sectPr.Name == W.sectPr)
                    {
                        AddSectionAndDependencies(doc, output, sectPr, images);
                        output.MainDocumentPart.GetXElementOrThrow().Element(W.body)?.Add(sectPr);
                    }
                }
            }
            else
            {
                FixUpSectionProperties(output);

                // Any sectPr elements that do not have headers and footers should take their headers
                // and footers from the *next* section, i.e., from the running section.
                XElement mxd = output.MainDocumentPart.GetXElementOrThrow();
                List<XElement> sections = mxd.Descendants(W.sectPr).Reverse().ToList();

                var cachedHeaderFooter = new[]
                {
                    new CachedHeaderFooter { Ref = W.headerReference, Type = "first" },
                    new CachedHeaderFooter { Ref = W.headerReference, Type = "even" },
                    new CachedHeaderFooter { Ref = W.headerReference, Type = "default" },
                    new CachedHeaderFooter { Ref = W.footerReference, Type = "first" },
                    new CachedHeaderFooter { Ref = W.footerReference, Type = "even" },
                    new CachedHeaderFooter { Ref = W.footerReference, Type = "default" },
                };

                var firstSection = true;

                foreach (XElement sect in sections)
                {
                    if (firstSection)
                    {
                        foreach (CachedHeaderFooter hf in cachedHeaderFooter)
                        {
                            XElement? referenceElement = sect
                                .Elements(hf.Ref)
                                .FirstOrDefault(z => (string) z.Attribute(W.type) == hf.Type);

                            if (referenceElement != null)
                            {
                                hf.CachedPartRid = (string) referenceElement.Attribute(R.id);
                            }
                        }

                        firstSection = false;
                    }
                    else
                    {
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "first");
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "even");
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "default");
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "first");
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "even");
                        CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "default");
                    }
                }
            }

            // Now can process PtOpenXml:Insert elements in headers / footers
            var sourceNum = 0;

            foreach (Source source in sources)
            {
                if (source.InsertId != null)
                {
                    // TODO: Revisit. This is an infinite loop.
                    while (true)
                    {
                        // this uses an overload of AppendDocument that takes a part.
                        // for each in main document part, header parts, footer parts
                        // are there any PtOpenXml.Insert elements in any of them?
                        // if so, then open and process all.
                        bool foundInHeadersFooters =
                            output.MainDocumentPart!.HeaderParts
                                .Any(hp => hp.GetXDocument()
                                    .Descendants(PtOpenXml.Insert)
                                    .Any(d => (string) d.Attribute(PtOpenXml.Id) == source.InsertId)) ||
                            output.MainDocumentPart.FooterParts
                                .Any(fp => fp.GetXDocument()
                                    .Descendants(PtOpenXml.Insert)
                                    .Any(d => (string) d.Attribute(PtOpenXml.Id) == source.InsertId));

                        if (foundInHeadersFooters)
                        {
                            using var streamDoc = new OpenXmlMemoryStreamDocument(source.WmlDocument);
                            using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();
                            doc.ThrowIfNotValid();

                            // Throws exceptions if a document contains unsupported content
                            TestForUnsupportedDocument(doc, sources.IndexOf(source));

                            List<OpenXmlPart> partList = output.MainDocumentPart.HeaderParts
                                .Concat(output.MainDocumentPart.FooterParts.Cast<OpenXmlPart>())
                                .ToList();

                            foreach (OpenXmlPart part in partList)
                            {
                                if (part.GetXElementOrThrow()
                                    .Descendants(PtOpenXml.Insert)
                                    .All(d => (string) d.Attribute(PtOpenXml.Id) != source.InsertId))
                                {
                                    continue;
                                }

                                List<XElement> contents = doc
                                    .MainDocumentPart
                                    .GetXElementOrThrow()
                                    .Elements(W.body)
                                    .Elements()
                                    .Skip(source.Start)
                                    .Take(source.Count)
                                    .ToList();

                                try
                                {
                                    AppendDocument(doc, output, part, contents, source.KeepSections, source.InsertId,
                                        images);
                                }
                                catch (DocumentBuilderInternalException dbie)
                                {
                                    if (dbie.Message.Contains("{0}"))
                                    {
                                        throw new DocumentBuilderException(string.Format(dbie.Message, sourceNum));
                                    }

                                    throw;
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                ++sourceNum;
            }

            if (sources.Any(s => s.KeepSections) && !output.MainDocumentPart.GetXElementOrThrow().Descendants(W.sectPr).Any())
            {
                using var streamDoc = new OpenXmlMemoryStreamDocument(sources[0].WmlDocument);
                using WordprocessingDocument doc = streamDoc.GetWordprocessingDocument();

                XElement? sectPr = doc
                    .MainDocumentPart
                    .GetXElementOrThrow()
                    .Elements(W.body)
                    .Elements()
                    .LastOrDefault();

                if (sectPr != null && sectPr.Name == W.sectPr)
                {
                    AddSectionAndDependencies(doc, output, sectPr, images);
                    output.MainDocumentPart.GetXElementOrThrow().Element(W.body)?.Add(sectPr);
                }
            }

            AdjustDocPrIds(output);

            WmlDocument? wmlGlossaryDocument = CoalesceGlossaryDocumentParts(sources);

            if (wmlGlossaryDocument != null)
            {
                WriteGlossaryDocumentPart(wmlGlossaryDocument, output, images);
            }

            foreach (OpenXmlPart part in output.GetAllParts())
            {
                // Save XDocument instances previously loaded with GetXDocument() or GetXElement().
                part.SaveXDocument();
            }
        }

        internal static IEnumerable<WmlDocument> SplitOnSections(WmlDocument doc)
        {
            using var streamDoc = new OpenXmlMemoryStreamDocument(doc);
            using WordprocessingDocument document = streamDoc.GetWordprocessingDocument();
            XElement rootXElement = document.MainDocumentPart.GetXElementOrThrow();

            IEnumerable<Atbid> divs = rootXElement
                .Elements(W.body)
                .Elements()
                .Select((p, i) => new Atbi
                {
                    BlockLevelContent = p,
                    Index = i,
                })
                .Rollup(new Atbid
                    {
                        BlockLevelContent = null,
                        Index = -1,
                        Div = 0,
                    },
                    (b, p) =>
                    {
                        XElement? elementBefore = b.BlockLevelContent?
                            .SiblingsBeforeSelfReverseDocumentOrder()
                            .FirstOrDefault();

                        return new Atbid
                        {
                            BlockLevelContent = b.BlockLevelContent,
                            Index = b.Index,
                            Div = elementBefore is not null && elementBefore.Descendants(W.sectPr).Any() ? p.Div + 1 : p.Div,
                        };
                    });

            IEnumerable<IGrouping<int, Atbid>> groups = divs.GroupAdjacent(b => b.Div);

            //List<TempSource> tempSourceList = groups
            //    .Select(g => new TempSource
            //    {
            //        Start = g.First().Index,
            //        Count = g.Count(),
            //    })
            //    .ToList();

            return groups
                .Select(g => new List<Source> { new(doc, g.First().Index, g.Count(), true) })
                .Select(BuildDocument)
                .Select(AdjustSectionBreak);

            //foreach (TempSource ts in tempSourceList)
            //{
            //    var sources = new List<Source>
            //    {
            //        new(doc, ts.Start, ts.Count, true),
            //    };

            //    WmlDocument newDoc = BuildDocument(sources);
            //    newDoc = AdjustSectionBreak(newDoc);
            //    yield return newDoc;
            //}
        }

        private static WmlDocument AdjustSectionBreak(WmlDocument doc)
        {
            using var streamDoc = new OpenXmlMemoryStreamDocument(doc);

            using (WordprocessingDocument document = streamDoc.GetWordprocessingDocument())
            {
                XElement rootXElement = document.MainDocumentPart.GetXElementOrThrow();

                XElement? lastElement = rootXElement
                    .Elements(W.body)
                    .Elements()
                    .LastOrDefault();

                if (lastElement == null || lastElement.Name == W.sectPr || !lastElement.Descendants(W.sectPr).Any())
                {
                    return streamDoc.GetModifiedWmlDocument();
                }

                rootXElement.Element(W.body)!.Add(lastElement.Descendants(W.sectPr).First());
                lastElement.Descendants(W.sectPr).Remove();

                if (lastElement.Elements().All(e => e.Name == W.pPr))
                {
                    lastElement.Remove();
                }

                document.MainDocumentPart!.SaveXDocument();
            }

            return streamDoc.GetModifiedWmlDocument();
        }

        // there are two scenarios that need to be handled
        // - if I find a style name that maps to a style ID different from one already mapped
        // - if a style name maps to a style ID that is already used for a different style
        // - then need to correct things
        //   - make a complete list of all things that need to be changed, for every correction
        //   - do the corrections all at one
        //   - mark the document as changed, and change it in the sources.
        private static List<Source> NormalizeStyleNamesAndIds(List<Source> sources)
        {
            var styleNameMap = new Dictionary<string, string>();
            var styleIds = new HashSet<string>();
            var newSources = new List<Source>();

            foreach (Source src in sources)
            {
                Source newSrc = AddAndRectify(src, styleNameMap, styleIds);
                newSources.Add(newSrc);
            }

            return newSources;
        }

        private static Source AddAndRectify(Source src, Dictionary<string, string> styleNameMap, HashSet<string> styleIds)
        {
            var modified = false;

            using var ms = new MemoryStream();
            ms.Write(src.WmlDocument.DocumentByteArray, 0, src.WmlDocument.DocumentByteArray.Length);

            using (WordprocessingDocument wDoc = WordprocessingDocument.Open(ms, true))
            {
                var correctionList = new Dictionary<string, string>();
                Dictionary<string, string> thisStyleNameMap = GetStyleNameMap(wDoc);

                foreach (KeyValuePair<string, string> pair in thisStyleNameMap)
                {
                    string styleName = pair.Key;
                    string styleId = pair.Value;

                    // if the styleNameMap does not contain an entry for this name
                    if (!styleNameMap.ContainsKey(styleName))
                    {
                        // if the id is already used
                        if (styleIds.Contains(styleId))
                        {
                            // this style uses a styleId that is used for another style.
                            // randomly generate new styleId
                            while (true)
                            {
                                string newStyleId = GenStyleIdFromStyleName(styleName);

                                if (!styleIds.Contains(newStyleId))
                                {
                                    correctionList.Add(styleId, newStyleId);
                                    styleNameMap.Add(styleName, newStyleId);
                                    styleIds.Add(newStyleId);
                                    break;
                                }
                            }
                        }
                        // otherwise we just add to the styleNameMap
                        else
                        {
                            styleNameMap.Add(styleName, styleId);
                            styleIds.Add(styleId);
                        }
                    }
                    // but if the styleNameMap does contain an entry for this name
                    else
                    {
                        // if the id is the same as the existing ID, then nothing to do
                        if (styleNameMap[styleName] == styleId)
                        {
                            continue;
                        }

                        correctionList.Add(styleId, styleNameMap[styleName]);
                    }
                }

                if (correctionList.Any())
                {
                    modified = true;
                    AdjustStyleIdsForDocument(wDoc, correctionList);
                }
            }

            if (modified)
            {
                var newWmlDocument = new WmlDocument(src.WmlDocument.FileName, ms.ToArray());

                var newSrc = new Source(newWmlDocument, src.Start, src.Count, src.KeepSections)
                {
                    DiscardHeadersAndFootersInKeptSections = src.DiscardHeadersAndFootersInKeptSections,
                    InsertId = src.InsertId,
                };

                return newSrc;
            }

            return src;
        }

        //
        // application/vnd.ms-word.styles.textEffects+xml                                                      styles/style/styleId
        // application/vnd.ms-word.styles.textEffects+xml                                                      styles/style/basedOn/val
        // application/vnd.ms-word.styles.textEffects+xml                                                      styles/style/link/val
        // application/vnd.ms-word.styles.textEffects+xml                                                      styles/style/next/val

        // application/vnd.ms-word.stylesWithEffects+xml                                                       styles/style/styleId
        // application/vnd.ms-word.stylesWithEffects+xml                                                       styles/style/basedOn/val
        // application/vnd.ms-word.stylesWithEffects+xml                                                       styles/style/link/val
        // application/vnd.ms-word.stylesWithEffects+xml                                                       styles/style/next/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml                         pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml                         rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml                         tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml                    pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml                    rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml                    tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml                         pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml                         rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml                         tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml                           pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml                           rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml                           tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml                        pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml                        rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml                        tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml                           pPr/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml                           rPr/rStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml                           tblPr/tblStyle/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml                        abstractNum/lvl/pStyle/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml                        abstractNum/numStyleLink/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml                        abstractNum/styleLink/val

        // application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml                         settings/clickAndTypeStyle/val

        // Name, not ID
        // ==============================================
        // application/vnd.ms-word.styles.textEffects+xml                                                      styles/style/name/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.stylesWithEffects+xml                styles/style/name/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml                           styles/style/name/val
        // application/vnd.ms-word.stylesWithEffects+xml                                                       styles/style/name/val
        // application/vnd.openxmlformats-officedocument.wordprocessingml.stylesWithEffects+xml                latentStyles/lsdException/name
        // application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml                           latentStyles/lsdException/name
        // application/vnd.ms-word.stylesWithEffects+xml                                                       latentStyles/lsdException/name
        // application/vnd.ms-word.styles.textEffects+xml                                                      latentStyles/lsdException/name
        //

        private static void AdjustStyleIdsForDocument(WordprocessingDocument wDoc, Dictionary<string, string> correctionList)
        {
            // Update styles part
            if (wDoc.MainDocumentPart!.StyleDefinitionsPart is { } styleDefinitionsPart)
            {
                UpdateStyleIdsForStylesPart(styleDefinitionsPart, correctionList);
            }

            if (wDoc.MainDocumentPart.StylesWithEffectsPart is { } stylesWithEffectsPart)
            {
                UpdateStyleIdsForStylesPart(stylesWithEffectsPart, correctionList);
            }

            // Update content parts
            UpdateStyleIdsForContentPart(wDoc.MainDocumentPart, correctionList);

            foreach (HeaderPart headerPart in wDoc.MainDocumentPart.HeaderParts)
            {
                UpdateStyleIdsForContentPart(headerPart, correctionList);
            }

            foreach (FooterPart footerPart in wDoc.MainDocumentPart.FooterParts)
            {
                UpdateStyleIdsForContentPart(footerPart, correctionList);
            }

            if (wDoc.MainDocumentPart.FootnotesPart is { } footnotesPart)
            {
                UpdateStyleIdsForContentPart(footnotesPart, correctionList);
            }

            if (wDoc.MainDocumentPart.EndnotesPart is { } endnotesPart)
            {
                UpdateStyleIdsForContentPart(endnotesPart, correctionList);
            }

            if (wDoc.MainDocumentPart.WordprocessingCommentsPart is { } commentsPart)
            {
                UpdateStyleIdsForContentPart(commentsPart, correctionList);
            }

            if (wDoc.MainDocumentPart.WordprocessingCommentsExPart is { } commentsExPart)
            {
                UpdateStyleIdsForContentPart(commentsExPart, correctionList);
            }

            // Update numbering part
            if (wDoc.MainDocumentPart.NumberingDefinitionsPart is { } numberingDefinitionsPart)
            {
                UpdateStyleIdsForNumberingPart(numberingDefinitionsPart, correctionList);
            }
        }

        private static void UpdateStyleIdsForNumberingPart(
            NumberingDefinitionsPart part,
            Dictionary<string, string> correctionList)
        {
            // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml     abstractNum/lvl/pStyle/val
            // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml     abstractNum/numStyleLink/val
            // application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml     abstractNum/styleLink/val

            XElement? rootXElement = part.GetXElement();

            if (rootXElement is null)
            {
                return;
            }

            var numAttributeChangeList = correctionList
                .Select(cor =>
                    new
                    {
                        NewId = cor.Value,
                        PStyleAttributesToChange = rootXElement
                            .Descendants(W.pStyle)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        NumStyleLinkAttributesToChange = rootXElement
                            .Descendants(W.numStyleLink)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        StyleLinkAttributesToChange = rootXElement
                            .Descendants(W.styleLink)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                    })
                .ToList();

            foreach (var item in numAttributeChangeList)
            {
                foreach (XAttribute att in item.PStyleAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.NumStyleLinkAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.StyleLinkAttributesToChange)
                {
                    att.Value = item.NewId;
                }
            }

            part.SaveXElement();
        }

        private static void UpdateStyleIdsForStylesPart(StylesPart part, Dictionary<string, string> correctionList)
        {
            // application/vnd.ms-word.styles.textEffects+xml   styles/style/styleId
            // application/vnd.ms-word.styles.textEffects+xml   styles/style/basedOn/val
            // application/vnd.ms-word.styles.textEffects+xml   styles/style/link/val
            // application/vnd.ms-word.styles.textEffects+xml   styles/style/next/val

            XElement? rootXElement = part.GetXElement();

            if (rootXElement is null)
            {
                return;
            }

            var styleAttributeChangeList = correctionList
                .Select(cor =>
                    new
                    {
                        NewId = cor.Value,
                        StyleIdAttributesToChange = rootXElement
                            .Elements(W.style)
                            .Attributes(W.styleId)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        BasedOnAttributesToChange = rootXElement
                            .Elements(W.style)
                            .Elements(W.basedOn)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        NextAttributesToChange = rootXElement
                            .Elements(W.style)
                            .Elements(W.next)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        LinkAttributesToChange = rootXElement
                            .Elements(W.style)
                            .Elements(W.link)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                    })
                .ToList();

            foreach (var item in styleAttributeChangeList)
            {
                foreach (XAttribute att in item.StyleIdAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.BasedOnAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.NextAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.LinkAttributesToChange)
                {
                    att.Value = item.NewId;
                }
            }

            part.SaveXElement();
        }

        private static void UpdateStyleIdsForContentPart(OpenXmlPart part, Dictionary<string, string> correctionList)
        {
            // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml pPr/pStyle/val
            // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml rPr/rStyle/val
            // application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml tblPr/tblStyle/val

            XElement? rootXElement = part.GetXElement();

            if (rootXElement is null)
            {
                return;
            }

            var mainAttributeChangeList = correctionList
                .Select(cor =>
                    new
                    {
                        NewId = cor.Value,
                        PStyleAttributesToChange = rootXElement
                            .Descendants(W.pStyle)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        RStyleAttributesToChange = rootXElement
                            .Descendants(W.rStyle)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                        TblStyleAttributesToChange = rootXElement
                            .Descendants(W.tblStyle)
                            .Attributes(W.val)
                            .Where(a => a.Value == cor.Key)
                            .ToList(),
                    })
                .ToList();

            foreach (var item in mainAttributeChangeList)
            {
                foreach (XAttribute att in item.PStyleAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.RStyleAttributesToChange)
                {
                    att.Value = item.NewId;
                }

                foreach (XAttribute att in item.TblStyleAttributesToChange)
                {
                    att.Value = item.NewId;
                }
            }

            part.SaveXElement();
        }

        private static string GenStyleIdFromStyleName(string styleName)
        {
            string newStyleId = styleName
                    .Replace("_", "")
                    .Replace("#", "")
                    .Replace(".", "") +
                (new Random().Next(990) + 9);

            return newStyleId;
        }

        private static Dictionary<string, string> GetStyleNameMap(WordprocessingDocument wDoc)
        {
            XElement? styles = wDoc.MainDocumentPart?.StyleDefinitionsPart?.GetXElement();

            return styles is null
                ? new Dictionary<string, string>()
                : styles
                    .Elements(W.style)
                    .Select(style => new
                    {
                        Name = (string?) style.Elements(W.name).Attributes(W.val).FirstOrDefault(),
                        StyleId = (string?) style.Attribute(W.styleId),
                    })
                    .Where(entry => entry.Name is not null && entry.StyleId is not null)
                    .ToDictionary(entry => entry.Name!, entry => entry.StyleId!);
        }

#if false
        At various locations in Open-Xml-PowerTools, you will find examples of Open XML markup that is associated with code associated with
        querying or generating that markup.  This is an example of the GlossaryDocumentPart.

<w:glossaryDocument xmlns:wpc = "http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas" xmlns:cx =
 "http://schemas.microsoft.com/office/drawing/2014/chartex" xmlns:cx1 =
 "http://schemas.microsoft.com/office/drawing/2015/9/8/chartex" xmlns:cx2 =
 "http://schemas.microsoft.com/office/drawing/2015/10/21/chartex" xmlns:cx3 =
 "http://schemas.microsoft.com/office/drawing/2016/5/9/chartex" xmlns:cx4 =
 "http://schemas.microsoft.com/office/drawing/2016/5/10/chartex" xmlns:cx5 =
 "http://schemas.microsoft.com/office/drawing/2016/5/11/chartex" xmlns:cx6 =
 "http://schemas.microsoft.com/office/drawing/2016/5/12/chartex" xmlns:cx7 =
 "http://schemas.microsoft.com/office/drawing/2016/5/13/chartex" xmlns:cx8 =
 "http://schemas.microsoft.com/office/drawing/2016/5/14/chartex" xmlns:mc =
 "http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:aink =
 "http://schemas.microsoft.com/office/drawing/2016/ink" xmlns:am3d =
 "http://schemas.microsoft.com/office/drawing/2017/model3d" xmlns:o = "urn:schemas-microsoft-com:office:office" xmlns:r =
 "http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:m =
 "http://schemas.openxmlformats.org/officeDocument/2006/math" xmlns:v = "urn:schemas-microsoft-com:vml" xmlns:wp14 =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing" xmlns:wp =
 "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" xmlns:w10 =
 "urn:schemas-microsoft-com:office:word" xmlns:w = "http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:w14 =
 "http://schemas.microsoft.com/office/word/2010/wordml" xmlns:w15 =
 "http://schemas.microsoft.com/office/word/2012/wordml" xmlns:w16cid =
 "http://schemas.microsoft.com/office/word/2016/wordml/cid" xmlns:w16se =
 "http://schemas.microsoft.com/office/word/2015/wordml/symex" xmlns:wpg =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingGroup" xmlns:wpi =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingInk" xmlns:wne =
 "http://schemas.microsoft.com/office/word/2006/wordml" xmlns:wps =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingShape" mc:Ignorable = "w14 w15 w16se w16cid wp14">
  <w:docParts>
    <w:docPart>
      <w:docPartPr>
        <w:name w:val = "CDE7B64C7BB446AE905B622B0A882EB6" />
        <w:category>
          <w:name w:val = "General" />
          <w:gallery w:val = "placeholder" />
        </w:category>
        <w:types>
          <w:type w:val = "bbPlcHdr" />
        </w:types>
        <w:behaviors>
          <w:behavior w:val = "content" />
        </w:behaviors>
        <w:guid w:val = "{13882A71-B5B7-4421-ACBB-9B61C61B3034}" />
      </w:docPartPr>
      <w:docPartBody>
        <w:p w:rsidR = "00004EEA" w:rsidRDefault = "00AD57F5" w:rsidP = "00AD57F5">
#endif

        private static void WriteGlossaryDocumentPart(
            WmlDocument wmlDocument,
            WordprocessingDocument output,
            List<ImageData> images)
        {
            using var glossaryStream = wmlDocument.ToMemoryStream();
            using WordprocessingDocument wordDocument = WordprocessingDocument.Open(glossaryStream, true);
            MainDocumentPart mainDocumentPart = wordDocument.MainDocumentPart!;
            XElement rootXElement = mainDocumentPart.GetXElementOrThrow();

            var outputGlossaryDocumentPart = output.MainDocumentPart!.AddNewPart<GlossaryDocumentPart>();

            outputGlossaryDocumentPart.SetXElement(new XElement(W.glossaryDocument,
                NamespaceAttributes,
                new XElement(W.docParts,
                    rootXElement.Descendants(W.docPart))));

            CopyGlossaryDocumentPartsToGd(wordDocument, output, rootXElement.Descendants(W.docPart).ToList(), images);
            CopyRelatedPartsForContentParts(mainDocumentPart, outputGlossaryDocumentPart, new[] { rootXElement }, images);
        }

        private static WmlDocument? CoalesceGlossaryDocumentParts(IEnumerable<Source> sources)
        {
            List<Source> allGlossaryDocuments = sources
                .Select(source => ExtractGlossaryDocument(source.WmlDocument))
                .OfType<WmlDocument>()
                .Select(source => new Source(source))
                .ToList();

            if (!allGlossaryDocuments.Any())
            {
                return null;
            }

            WmlDocument coalescedRaw = BuildDocument(allGlossaryDocuments);

            // Now need to do some fix up
            using var stream = coalescedRaw.ToMemoryStream();

            using (WordprocessingDocument wDoc = WordprocessingDocument.Open(stream, true))
            {
                XElement mainXDoc = wDoc.MainDocumentPart.GetXElementOrThrow();

                var newBody = new XElement(W.body,
                    new XElement(W.docParts,
                        mainXDoc.Elements(W.body).Elements(W.docParts).Elements(W.docPart)));

                mainXDoc.Element(W.body)?.ReplaceWith(newBody);

                wDoc.MainDocumentPart!.SaveXDocument();
            }

            var coalescedGlossaryDocument = new WmlDocument("Coalesced.docx", stream.ToArray());

            return coalescedGlossaryDocument;
        }

#if false
        At various locations in Open-Xml-PowerTools, you will find examples of Open XML markup that is associated with code associated with
        querying or generating that markup.  This is an example of the Custom XML Properties part.

<ds:datastoreItem ds:itemID = "{1337A0C2-E6EE-4612-ACA5-E0E5A513381D}" xmlns:ds =
 "http://schemas.openxmlformats.org/officeDocument/2006/customXml">
  <ds:schemaRefs />
</ds:datastoreItem>
#endif

        private static void CopySpecifiedCustomXmlParts(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument output,
            DocumentBuilderSettings settings)
        {
            if (!settings.CustomXmlGuidList.Any())
            {
                return;
            }

            foreach (CustomXmlPart customXmlPart in sourceDocument.MainDocumentPart!.CustomXmlParts)
            {
                OpenXmlPart? propertyPart = customXmlPart
                    .Parts
                    .Select(p => p.OpenXmlPart)
                    .FirstOrDefault(p => p.ContentType == "application/vnd.openxmlformats-officedocument.customXmlProperties+xml");

                if (propertyPart == null)
                {
                    continue;
                }

                XElement? propertyPartDoc = propertyPart.GetXElement();
                var itemId = (string?) propertyPartDoc?.Attribute(DS.itemID);

                if (itemId == null)
                {
                    continue;
                }

                itemId = itemId.Trim('{', '}');

                if (!settings.CustomXmlGuidList.Contains(itemId))
                {
                    continue;
                }

                // TODO: Revisit. Why use Add() instead of SetXElement()? See below for similar case.
                CustomXmlPart newPart = output.MainDocumentPart!.AddCustomXmlPart(customXmlPart.ContentType);
                newPart.GetXDocument().Add(customXmlPart.GetXElement());

                foreach (OpenXmlPart propPart in customXmlPart.Parts.Select(p => p.OpenXmlPart))
                {
                    var newPropPart = newPart.AddNewPart<CustomXmlPropertiesPart>();
                    newPropPart.GetXDocument().Add(propPart.GetXElement());
                }
            }
        }

        private static void RemoveHeadersAndFootersFromSections(WordprocessingDocument doc)
        {
            XDocument mdXDoc = doc.MainDocumentPart!.GetXDocument();
            List<XElement> sections = mdXDoc.Descendants(W.sectPr).ToList();

            foreach (XElement sect in sections)
            {
                sect.Elements(W.headerReference).Remove();
                sect.Elements(W.footerReference).Remove();
            }

            doc.MainDocumentPart!.SaveXDocument();
        }

        private static void ProcessSectionsForLinkToPreviousHeadersAndFooters(WordprocessingDocument doc)
        {
            CachedHeaderFooter[] cachedHeaderFooter =
            {
                new() { Ref = W.headerReference, Type = "first" },
                new() { Ref = W.headerReference, Type = "even" },
                new() { Ref = W.headerReference, Type = "default" },
                new() { Ref = W.footerReference, Type = "first" },
                new() { Ref = W.footerReference, Type = "even" },
                new() { Ref = W.footerReference, Type = "default" },
            };

            XDocument mdXDoc = doc.MainDocumentPart!.GetXDocument();
            List<XElement> sections = mdXDoc.Descendants(W.sectPr).ToList();
            var firstSection = true;

            foreach (XElement sect in sections)
            {
                if (firstSection)
                {
                    XElement? headerFirst = FindReference(sect, W.headerReference, "first");
                    XElement? headerDefault = FindReference(sect, W.headerReference, "default");
                    XElement? headerEven = FindReference(sect, W.headerReference, "even");
                    XElement? footerFirst = FindReference(sect, W.footerReference, "first");
                    XElement? footerDefault = FindReference(sect, W.footerReference, "default");
                    XElement? footerEven = FindReference(sect, W.footerReference, "even");

                    if (headerEven == null)
                    {
                        if (headerDefault != null)
                        {
                            AddReferenceToExistingHeaderOrFooter(sect, (string) headerDefault.Attribute(R.id), W.headerReference,
                                "even");
                        }
                        else
                        {
                            InitEmptyHeaderOrFooter(doc.MainDocumentPart!, sect, W.headerReference, "even");
                        }
                    }

                    if (headerFirst == null)
                    {
                        if (headerDefault != null)
                        {
                            AddReferenceToExistingHeaderOrFooter(sect, (string) headerDefault.Attribute(R.id), W.headerReference,
                                "first");
                        }
                        else
                        {
                            InitEmptyHeaderOrFooter(doc.MainDocumentPart!, sect, W.headerReference, "first");
                        }
                    }

                    if (footerEven == null)
                    {
                        if (footerDefault != null)
                        {
                            AddReferenceToExistingHeaderOrFooter(sect, (string) footerDefault.Attribute(R.id), W.footerReference,
                                "even");
                        }
                        else
                        {
                            InitEmptyHeaderOrFooter(doc.MainDocumentPart!, sect, W.footerReference, "even");
                        }
                    }

                    if (footerFirst == null)
                    {
                        if (footerDefault != null)
                        {
                            AddReferenceToExistingHeaderOrFooter(sect, (string) footerDefault.Attribute(R.id), W.footerReference,
                                "first");
                        }
                        else
                        {
                            InitEmptyHeaderOrFooter(doc.MainDocumentPart!, sect, W.footerReference, "first");
                        }
                    }

                    foreach (CachedHeaderFooter hf in cachedHeaderFooter)
                    {
                        if (sect.Elements(hf.Ref).FirstOrDefault(z => (string) z.Attribute(W.type) == hf.Type) == null)
                        {
                            InitEmptyHeaderOrFooter(doc.MainDocumentPart!, sect, hf.Ref, hf.Type);
                        }

                        XElement? reference = sect.Elements(hf.Ref).FirstOrDefault(z => (string) z.Attribute(W.type) == hf.Type);

                        if (reference == null)
                        {
                            throw new OpenXmlPowerToolsException("Internal error");
                        }

                        hf.CachedPartRid = (string) reference.Attribute(R.id);
                    }

                    firstSection = false;
                    continue;
                }

                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "first");
                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "even");
                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.headerReference, "default");
                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "first");
                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "even");
                CopyOrCacheHeaderOrFooter(cachedHeaderFooter, sect, W.footerReference, "default");
            }

            doc.MainDocumentPart!.SaveXDocument();
        }

        private static void CopyOrCacheHeaderOrFooter(
            IEnumerable<CachedHeaderFooter> cachedHeaderFooter,
            XElement sect,
            XName referenceXName,
            string type)
        {
            XElement? referenceElement = FindReference(sect, referenceXName, type);

            if (referenceElement == null)
            {
                string cachedPartRid = cachedHeaderFooter
                    .First(z => z.Ref == referenceXName && z.Type == type)
                    .CachedPartRid!;

                AddReferenceToExistingHeaderOrFooter(sect, cachedPartRid, referenceXName, type);
            }
            else
            {
                CachedHeaderFooter cachedPart = cachedHeaderFooter.First(z => z.Ref == referenceXName && z.Type == type);
                cachedPart.CachedPartRid = (string) referenceElement.Attribute(R.id);
            }
        }

        private static XElement? FindReference(XElement sect, XName reference, string type)
        {
            return sect.Elements(reference)
                .FirstOrDefault(z => (string) z.Attribute(W.type) == type);
        }

        private static void AddReferenceToExistingHeaderOrFooter(
            XElement sect,
            string rId,
            XName reference,
            string toType)
        {
            if (reference == W.headerReference)
            {
                var referenceToAdd = new XElement(W.headerReference,
                    new XAttribute(W.type, toType),
                    new XAttribute(R.id, rId));

                sect.AddFirst(referenceToAdd);
            }
            else
            {
                var referenceToAdd = new XElement(W.footerReference,
                    new XAttribute(W.type, toType),
                    new XAttribute(R.id, rId));

                sect.AddFirst(referenceToAdd);
            }
        }

        private static void InitEmptyHeaderOrFooter(
            MainDocumentPart mainDocumentPart,
            XElement sect,
            XName referenceXName,
            string toType)
        {
            if (referenceXName == W.headerReference)
            {
                XDocument xDoc = XDocument.Parse(@"<?xml version='1.0' encoding='utf-8' standalone='yes'?>
                    <w:hdr xmlns:wpc='http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas'
                           xmlns:mc='http://schemas.openxmlformats.org/markup-compatibility/2006'
                           xmlns:o='urn:schemas-microsoft-com:office:office'
                           xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
                           xmlns:m='http://schemas.openxmlformats.org/officeDocument/2006/math'
                           xmlns:v='urn:schemas-microsoft-com:vml'
                           xmlns:wp14='http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing'
                           xmlns:wp='http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing'
                           xmlns:w10='urn:schemas-microsoft-com:office:word'
                           xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
                           xmlns:w14='http://schemas.microsoft.com/office/word/2010/wordml'
                           xmlns:w15='http://schemas.microsoft.com/office/word/2012/wordml'
                           xmlns:wpg='http://schemas.microsoft.com/office/word/2010/wordprocessingGroup'
                           xmlns:wpi='http://schemas.microsoft.com/office/word/2010/wordprocessingInk'
                           xmlns:wne='http://schemas.microsoft.com/office/word/2006/wordml'
                           xmlns:wps='http://schemas.microsoft.com/office/word/2010/wordprocessingShape'
                           mc:Ignorable='w14 w15 wp14'>
                      <w:p>
                        <w:pPr>
                          <w:pStyle w:val='Header' />
                        </w:pPr>
                        <w:r>
                          <w:t></w:t>
                        </w:r>
                      </w:p>
                    </w:hdr>");

                var newHeaderPart = mainDocumentPart.AddNewPart<HeaderPart>();
                newHeaderPart.SetXDocument(xDoc);

                var referenceToAdd = new XElement(W.headerReference,
                    new XAttribute(W.type, toType),
                    new XAttribute(R.id, mainDocumentPart.GetIdOfPart(newHeaderPart)));

                sect.AddFirst(referenceToAdd);
            }
            else
            {
                XDocument xDoc = XDocument.Parse(@"<?xml version='1.0' encoding='utf-8' standalone='yes'?>
                    <w:ftr xmlns:wpc='http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas'
                           xmlns:mc='http://schemas.openxmlformats.org/markup-compatibility/2006'
                           xmlns:o='urn:schemas-microsoft-com:office:office'
                           xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
                           xmlns:m='http://schemas.openxmlformats.org/officeDocument/2006/math'
                           xmlns:v='urn:schemas-microsoft-com:vml'
                           xmlns:wp14='http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing'
                           xmlns:wp='http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing'
                           xmlns:w10='urn:schemas-microsoft-com:office:word'
                           xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
                           xmlns:w14='http://schemas.microsoft.com/office/word/2010/wordml'
                           xmlns:w15='http://schemas.microsoft.com/office/word/2012/wordml'
                           xmlns:wpg='http://schemas.microsoft.com/office/word/2010/wordprocessingGroup'
                           xmlns:wpi='http://schemas.microsoft.com/office/word/2010/wordprocessingInk'
                           xmlns:wne='http://schemas.microsoft.com/office/word/2006/wordml'
                           xmlns:wps='http://schemas.microsoft.com/office/word/2010/wordprocessingShape'
                           mc:Ignorable='w14 w15 wp14'>
                      <w:p>
                        <w:pPr>
                          <w:pStyle w:val='Footer' />
                        </w:pPr>
                        <w:r>
                          <w:t></w:t>
                        </w:r>
                      </w:p>
                    </w:ftr>");

                var newFooterPart = mainDocumentPart.AddNewPart<FooterPart>();
                newFooterPart.SetXDocument(xDoc);

                var referenceToAdd = new XElement(W.footerReference,
                    new XAttribute(W.type, toType),
                    new XAttribute(R.id, mainDocumentPart.GetIdOfPart(newFooterPart)));

                sect.AddFirst(referenceToAdd);
            }
        }

        private static void TestPartForUnsupportedContent(OpenXmlPart part, int sourceNumber)
        {
            XNamespace[] obsoleteNamespaces =
            {
                XNamespace.Get("http://schemas.microsoft.com/office/word/2007/5/30/wordml"),
                XNamespace.Get("http://schemas.microsoft.com/office/word/2008/9/16/wordprocessingDrawing"),
                XNamespace.Get("http://schemas.microsoft.com/office/word/2009/2/wordml"),
            };

            XDocument xDoc = part.GetXDocument();

            XElement? invalidElement = xDoc
                .Descendants()
                .FirstOrDefault(d =>
                {
                    bool b = d.Name == W.subDoc ||
                        d.Name == W.control ||
                        d.Name == W.altChunk ||
                        d.Name.LocalName == "contentPart" ||
                        obsoleteNamespaces.Contains(d.Name.Namespace);

                    bool b2 = b ||
                        d.Attributes().Any(a => obsoleteNamespaces.Contains(a.Name.Namespace));

                    return b2;
                });

            if (invalidElement == null)
            {
                return;
            }

            if (invalidElement.Name == W.subDoc)
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains sub document");
            }

            if (invalidElement.Name == W.control)
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains ActiveX controls");
            }

            if (invalidElement.Name == W.altChunk)
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains altChunk");
            }

            if (invalidElement.Name.LocalName == "contentPart")
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains contentPart content");
            }

            if (obsoleteNamespaces.Contains(invalidElement.Name.Namespace) ||
                invalidElement.Attributes().Any(a => obsoleteNamespaces.Contains(a.Name.Namespace)))
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains obsolete namespace");
            }
        }

        //What does not work:
        //- sub docs
        //- bidi text appears to work but has not been tested
        //- languages other than en-us appear to work but have not been tested
        //- documents with activex controls
        //- mail merge source documents (look for dataSource in settings)
        //- documents with ink
        //- documents with frame sets and frames
        private static void TestForUnsupportedDocument(WordprocessingDocument doc, int sourceNumber)
        {
#if TestForUnsupportedDocuments

            // The MainDocumentPart will exist, so we do not have to check for null.
            MainDocumentPart mainDocumentPart = doc.MainDocumentPart!;

            XElement? document = mainDocumentPart.GetXElement();

            if (document is null)
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is an invalid document - MainDocumentPart contains no content.");
            }

            if (document.Name.NamespaceName == "http://purl.oclc.org/ooxml/wordprocessingml/main")
            {
                throw new DocumentBuilderException($"Source {sourceNumber} is saved in strict mode, not supported");
            }

            // note: if ever want to support section changes, need to address the code that rationalizes
            // headers and footers, propagating to sections that inherit headers/footers from prev section
            if (document.Descendants().Any(d => d.Name == W.sectPrChange))
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} contains section changes (w:sectPrChange), not supported");
            }

            TestPartForUnsupportedContent(mainDocumentPart, sourceNumber);

            foreach (HeaderPart hdr in mainDocumentPart.HeaderParts)
            {
                TestPartForUnsupportedContent(hdr, sourceNumber);
            }

            foreach (FooterPart ftr in mainDocumentPart.FooterParts)
            {
                TestPartForUnsupportedContent(ftr, sourceNumber);
            }

            if (mainDocumentPart.FootnotesPart is not null)
            {
                TestPartForUnsupportedContent(mainDocumentPart.FootnotesPart, sourceNumber);
            }

            if (mainDocumentPart.EndnotesPart is not null)
            {
                TestPartForUnsupportedContent(mainDocumentPart.EndnotesPart, sourceNumber);
            }

            if (mainDocumentPart.DocumentSettingsPart is not null &&
                mainDocumentPart.DocumentSettingsPart
                    .GetXDocument()
                    .Descendants()
                    .Any(d => d.Name == W.src ||
                        d.Name == W.recipientData ||
                        d.Name == W.mailMerge))
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is unsupported document - contains Mail Merge content");
            }

            if (mainDocumentPart.WebSettingsPart != null &&
                mainDocumentPart.WebSettingsPart.GetXDocument().Descendants().Any(d => d.Name == W.frameset))
            {
                throw new DocumentBuilderException($"Source {sourceNumber} is unsupported document - contains a frameset");
            }

            List<XElement> numberingElements = mainDocumentPart
                .GetXDocument()
                .Descendants(W.numPr)
                .Where(n =>
                {
                    bool zeroId = (int?) n.Attribute(W.id) == 0;
                    bool hasChildInsId = n.Elements(W.ins).Any();

                    return !zeroId && !hasChildInsId;
                })
                .ToList();

            if (numberingElements.Any() &&
                mainDocumentPart.NumberingDefinitionsPart == null)
            {
                throw new DocumentBuilderException(
                    $"Source {sourceNumber} is invalid document - contains numbering markup but no numbering part");
            }
#endif
        }

        private static void FixUpSectionProperties(WordprocessingDocument newDocument)
        {
            XElement body = newDocument.MainDocumentPart!.GetXElementOrThrow().Elements(W.body).Single();

            List<XElement> sectionPropertiesToMove = body
                .Elements()
                .Take(body.Elements().Count() - 1)
                .Where(e => e.Name == W.sectPr)
                .ToList();

            foreach (XElement s in sectionPropertiesToMove)
            {
                XElement p = s.SiblingsBeforeSelfReverseDocumentOrder().First();

                if (p.Element(W.pPr) == null)
                {
                    p.AddFirst(new XElement(W.pPr));
                }

                p.Element(W.pPr)!.Add(s);
            }

            foreach (XElement s in sectionPropertiesToMove)
            {
                s.Remove();
            }
        }

        private static void AddSectionAndDependencies(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            XElement sectionMarkup,
            List<ImageData> images)
        {
            AddSectionAndDependencies<HeaderPart>(sourceDocument, newDocument, sectionMarkup, images);
            AddSectionAndDependencies<FooterPart>(sourceDocument, newDocument, sectionMarkup, images);
        }

        private static void AddSectionAndDependencies<TOpenXmlPart>(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            XElement sectionMarkup,
            List<ImageData> images)
            where TOpenXmlPart : OpenXmlPart, IFixedContentTypePart
        {
            IEnumerable<XElement> references = typeof(TOpenXmlPart) switch
            {
                { Name: nameof(HeaderPart) } => sectionMarkup.Elements(W.headerReference),
                { Name: nameof(FooterPart) } => sectionMarkup.Elements(W.footerReference),
                _ => throw new ArgumentOutOfRangeException(),
            };

            foreach (XElement reference in references)
            {
                string oldRid = reference.Attribute(R.id)!.Value;

                var oldPart = sourceDocument.MainDocumentPart!.GetPart<TOpenXmlPart>(oldRid);
                XElement rootXElement = oldPart.GetXElementOrThrow();

                CopyNumbering(sourceDocument, newDocument, new [] { rootXElement }, images);

                var newPart = newDocument.MainDocumentPart!.AddNewPart<TOpenXmlPart>();
                newPart.SetXElement(rootXElement);

                reference.SetAttributeValue(R.id, newDocument.MainDocumentPart.GetIdOfPart(newPart));

                AddRelationships(oldPart, newPart, new [] { rootXElement });
                CopyRelatedPartsForContentParts(oldPart, newPart, new [] { rootXElement }, images);
            }
        }

        private static TOpenXmlPart GetPart<TOpenXmlPart>(this OpenXmlPart parentPart, string rid)
            where TOpenXmlPart : OpenXmlPart
        {
            try
            {
                return (TOpenXmlPart) parentPart.GetPartById(rid);
            }
            catch (ArgumentOutOfRangeException e)
            {
                throw new DocumentBuilderException($"Part with r:id=\"{rid}\" does not exist.", e);
            }
            catch (InvalidCastException e)
            {
                throw new DocumentBuilderException($"Part with r:id=\"{rid}\" does not have the expected type.", e);
            }
        }

        private static void MergeStyles(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            XDocument fromStyles,
            XDocument toStyles,
            IEnumerable<XElement> newContent)
        {
#if MergeStylesWithSameNames
            var newIds = new Dictionary<string, string>();
#endif
            if (fromStyles.Root is null || toStyles.Root is null)
            {
                return;
            }

            foreach (XElement fromStyle in fromStyles.Root.Elements(W.style))
            {
                var fromStyleId = (string) fromStyle.Attribute(W.styleId);
                var fromName = (string) fromStyle.Elements(W.name).Attributes(W.val).FirstOrDefault();

                XElement? toStyle = toStyles
                    .Root
                    .Elements(W.style)
                    .FirstOrDefault(st => (string) st.Elements(W.name).Attributes(W.val).FirstOrDefault() == fromName);

                if (toStyle is null)
                {
#if MergeStylesWithSameNames
                    string? fromLinkVal = fromStyle.Element(W.link)?.Attribute(W.val)?.Value;

                    if (fromLinkVal is not null && newIds.TryGetValue(fromLinkVal, out string linkedId))
                    {
                        XElement toLinkedStyle = toStyles.Root
                            .Elements(W.style)
                            .First(style => style.Attribute(W.styleId)?.Value == linkedId);

                        string? toLinkVal = toLinkedStyle.Element(W.link)?.Attribute(W.val)?.Value;

                        if (toLinkVal is not null)
                        {
                            newIds.Add(fromStyleId, toLinkVal);
                        }

                        continue;
                    }

                    //string name = (string)style.Elements(W.name).Attributes(W.val).FirstOrDefault();
                    //var namedStyle = toStyles
                    //    .Root
                    //    .Elements(W.style)
                    //    .Where(st => st.Element(W.name) != null)
                    //    .FirstOrDefault(o => (string)o.Element(W.name).Attribute(W.val) == name);
                    //if (namedStyle != null)
                    //{
                    //    if (! newIds.ContainsKey(fromId))
                    //        newIds.Add(fromId, namedStyle.Attribute(W.styleId).Value);
                    //    continue;
                    //}
#endif

                    var number = 1;
                    var abstractNumber = 0;
                    XDocument? oldNumbering = null;
                    XDocument? newNumbering = null;

                    foreach (XElement numReference in fromStyle.Descendants(W.numPr))
                    {
                        XElement? idElement = numReference.Descendants(W.numId).FirstOrDefault();

                        if (idElement is null)
                        {
                            continue;
                        }

                        oldNumbering ??= sourceDocument.MainDocumentPart!.NumberingDefinitionsPart is not null
                            ? sourceDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument()
                            : new XDocument(new XDeclaration(OnePointZero, Utf8, Yes),
                                new XElement(W.numbering, NamespaceAttributes));

                        if (newNumbering is null)
                        {
                            if (newDocument.MainDocumentPart!.NumberingDefinitionsPart is not null)
                            {
                                newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();

                                List<int> numIds = newNumbering
                                    .Root!
                                    .Elements(W.num)
                                    .Select(f => (int) f.Attribute(W.numId))
                                    .ToList();

                                if (numIds.Any())
                                {
                                    number = numIds.Max() + 1;
                                }

                                numIds = newNumbering
                                    .Root
                                    .Elements(W.abstractNum)
                                    .Select(f => (int) f.Attribute(W.abstractNumId))
                                    .ToList();

                                if (numIds.Any())
                                {
                                    abstractNumber = numIds.Max() + 1;
                                }
                            }
                            else
                            {
                                var numberingDefinitionsPart = newDocument.MainDocumentPart.AddNewPart<NumberingDefinitionsPart>();
                                newNumbering = numberingDefinitionsPart.GetXDocument();
                                newNumbering.Add(new XElement(W.numbering, NamespaceAttributes));
                            }
                        }

                        string? numId = idElement.Attribute(W.val)?.Value;

                        if (numId == "0")
                        {
                            continue;
                        }

                        XElement? element = oldNumbering
                            .Descendants()
                            .Elements(W.num)
                            .FirstOrDefault(p => (string) p.Attribute(W.numId) == numId);

                        if (element is null)
                        {
                            continue;
                        }

                        // Copy abstract numbering element, if necessary (use matching NSID)
                        string? abstractNumId = element
                            .Elements(W.abstractNumId)
                            .First()
                            .Attribute(W.val)
                            ?
                            .Value;

                        XElement? abstractElement = oldNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .FirstOrDefault(p => (string) p.Attribute(W.abstractNumId) == abstractNumId);

                        if (abstractElement == null)
                        {
                            continue;
                        }

                        XElement? nsidElement = abstractElement.Element(W.nsid);
                        string? abstractNsid = nsidElement?.Attribute(W.val)?.Value;

                        XElement? newAbstractElement = newNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                            .Where(p =>
                            {
                                XElement? thisNsidElement = p.Element(W.nsid);
                                return thisNsidElement != null && (string) thisNsidElement.Attribute(W.val) == abstractNsid;
                            })
                            .FirstOrDefault();

                        if (newAbstractElement == null)
                        {
                            newAbstractElement = new XElement(abstractElement);
                            newAbstractElement.SetAttributeValue(W.abstractNumId, abstractNumber);
                            abstractNumber++;

                            if (newNumbering.Root!.Elements(W.abstractNum).Any())
                            {
                                newNumbering.Root.Elements(W.abstractNum).Last().AddAfterSelf(newAbstractElement);
                            }
                            else
                            {
                                newNumbering.Root.Add(newAbstractElement);
                            }

                            foreach (XElement pictId in newAbstractElement.Descendants(W.lvlPicBulletId))
                            {
                                var bulletId = (string) pictId.Attribute(W.val);

                                // TODO: Revisit. Was FirstOrDefault().
                                XElement numPicBullet = oldNumbering
                                    .Descendants(W.numPicBullet)
                                    .First(d => (string) d.Attribute(W.numPicBulletId) == bulletId);

                                int maxNumPicBulletId =
                                    new[] { -1 }
                                        .Concat(newNumbering
                                            .Descendants(W.numPicBullet)
                                            .Attributes(W.numPicBulletId)
                                            .Select(a => (int) a))
                                        .Max() +
                                    1;

                                var newNumPicBullet = new XElement(numPicBullet);
                                newNumPicBullet.SetAttributeValue(W.numPicBulletId, maxNumPicBulletId);
                                pictId.SetAttributeValue(W.val, maxNumPicBulletId);
                                newNumbering.Root.AddFirst(newNumPicBullet);
                            }
                        }

                        string? newAbstractId = newAbstractElement.Attribute(W.abstractNumId)?.Value;

                        // Copy numbering element, if necessary (use matching element with no overrides)
                        XElement? newElement = null;

                        if (!element.Elements(W.lvlOverride).Any())
                        {
                            newElement = newNumbering
                                .Descendants()
                                .Elements(W.num)
                                .FirstOrDefault(p =>
                                    !p.Elements(W.lvlOverride).Any() &&
                                    (string) p.Elements(W.abstractNumId).First().Attribute(W.val) == newAbstractId);
                        }

                        if (newElement == null)
                        {
                            newElement = new XElement(element);
                            newElement.Elements(W.abstractNumId).First().SetAttributeValue(W.val, newAbstractId);
                            newElement.SetAttributeValue(W.numId, number);
                            number++;

                            newNumbering.Root!.Add(newElement);
                        }

                        idElement.SetAttributeValue(W.val, newElement.Attribute(W.numId)!.Value);
                    }

                    var newStyle = new XElement(fromStyle);
                    // get rid of anything not in the w: namespace
                    newStyle.Descendants().Where(d => d.Name.NamespaceName != W.w).Remove();
                    newStyle.Descendants().Attributes().Where(d => d.Name.NamespaceName != W.w).Remove();
                    toStyles.Root.Add(newStyle);
                }
                else
                {
                    var toId = (string) toStyle.Attribute(W.styleId);

                    if (fromStyleId == toId)
                    {
                        continue;
                    }

                    if (!newIds.ContainsKey(fromStyleId))
                    {
                        newIds.Add(fromStyleId, toId);
                    }
                }
            }

#if MergeStylesWithSameNames
            if (newIds.Count <= 0)
            {
                return;
            }

            foreach (XElement style in toStyles.Root.Elements(W.style))
            {
                ConvertToNewId(style.Element(W.basedOn), newIds);
                ConvertToNewId(style.Element(W.next), newIds);
            }

            foreach (XElement item in newContent
                         .DescendantsAndSelf()
                         .Where(d => d.Name == W.pStyle || d.Name == W.rStyle || d.Name == W.tblStyle))
            {
                ConvertToNewId(item, newIds);
            }

            if (newDocument.MainDocumentPart!.NumberingDefinitionsPart is not null)
            {
                XDocument newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();
                ConvertNumberingPartToNewIds(newNumbering, newIds);
            }

            // Convert source document, since numberings will be copied over after styles.
            if (sourceDocument.MainDocumentPart!.NumberingDefinitionsPart is not null)
            {
                XDocument sourceNumbering = sourceDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();
                ConvertNumberingPartToNewIds(sourceNumbering, newIds);
            }
#endif
        }

        private static void MergeLatentStyles(XDocument fromStyles, XDocument toStyles)
        {
            XElement? fromLatentStyles = fromStyles.Descendants(W.latentStyles).FirstOrDefault();

            if (fromLatentStyles is null)
            {
                return;
            }

            XElement? toLatentStyles = toStyles.Descendants(W.latentStyles).FirstOrDefault();

            if (toLatentStyles is null)
            {
                var newLatentStylesElement = new XElement(W.latentStyles,
                    fromLatentStyles.Attributes());

                XElement? globalDefaults = toStyles
                    .Descendants(W.docDefaults)
                    .FirstOrDefault();

                if (globalDefaults is null)
                {
                    XElement? firstStyle = toStyles
                        .Root!
                        .Elements(W.style)
                        .FirstOrDefault();

                    if (firstStyle is null)
                    {
                        toStyles.Root.Add(newLatentStylesElement);
                    }
                    else
                    {
                        firstStyle.AddBeforeSelf(newLatentStylesElement);
                    }
                }
                else
                {
                    globalDefaults.AddAfterSelf(newLatentStylesElement);
                }
            }

            toLatentStyles = toStyles.Descendants(W.latentStyles).FirstOrDefault();

            if (toLatentStyles == null)
            {
                throw new DocumentBuilderException("Internal error");
            }

            var toStylesHash = new HashSet<string>();

            foreach (XElement lse in toLatentStyles.Elements(W.lsdException))
            {
                toStylesHash.Add((string) lse.Attribute(W.name));
            }

            foreach (XElement fls in fromLatentStyles.Elements(W.lsdException))
            {
                var name = (string) fls.Attribute(W.name);

                if (toStylesHash.Contains(name))
                {
                    continue;
                }

                toLatentStyles.Add(fls);
                toStylesHash.Add(name);
            }

            int count = toLatentStyles
                .Elements(W.lsdException)
                .Count();

            toLatentStyles.SetAttributeValue(W.count, count);
        }

        private static void MergeDocDefaultStyles(XDocument xDocument, XDocument newXDoc)
        {
            IEnumerable<XElement> docDefaultStyles = xDocument.Descendants(W.docDefaults);

            foreach (XElement docDefaultStyle in docDefaultStyles)
            {
                newXDoc.Root?.Add(docDefaultStyle);
            }
        }

#if MergeStylesWithSameNames
        private static void ConvertToNewId(XElement? element, Dictionary<string, string> newIds)
        {
            XAttribute? valueAttribute = element?.Attribute(W.val);

            if (valueAttribute is null)
            {
                return;
            }

            if (newIds.TryGetValue(valueAttribute.Value, out string newId))
            {
                valueAttribute.Value = newId;
            }
        }

        private static void ConvertNumberingPartToNewIds(XDocument newNumbering, Dictionary<string, string> newIds)
        {
            foreach (XElement abstractNum in newNumbering.Root!.Elements(W.abstractNum))
            {
                ConvertToNewId(abstractNum.Element(W.styleLink), newIds);
                ConvertToNewId(abstractNum.Element(W.numStyleLink), newIds);
            }

            foreach (XElement item in newNumbering
                         .Descendants()
                         .Where(d => d.Name == W.pStyle || d.Name == W.rStyle || d.Name == W.tblStyle))
            {
                ConvertToNewId(item, newIds);
            }
        }
#endif

        private static void MergeFontTables(XDocument fromFontTable, XDocument toFontTable)
        {
            foreach (XElement font in fromFontTable.Root!.Elements(W.font))
            {
                string? name = font.Attribute(W.name)?.Value;

                if (toFontTable.Root!.Elements(W.font).All(o => o.Attribute(W.name)?.Value != name))
                {
                    toFontTable.Root.Add(new XElement(font));
                }
            }
        }

        private static void CopyStylesAndFonts(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IReadOnlyCollection<XElement> newContent)
        {
            // Copy all styles to the new document
            if (sourceDocument.MainDocumentPart!.StyleDefinitionsPart is not null)
            {
                XDocument oldStyles = sourceDocument.MainDocumentPart.StyleDefinitionsPart.GetXDocument();

                if (newDocument.MainDocumentPart!.StyleDefinitionsPart is null)
                {
                    newDocument.MainDocumentPart.AddNewPart<StyleDefinitionsPart>();
                    XDocument newStyles = newDocument.MainDocumentPart.StyleDefinitionsPart!.GetXDocument();
                    newStyles.Add(oldStyles.Root);
                }
                else
                {
                    XDocument newStyles = newDocument.MainDocumentPart.StyleDefinitionsPart.GetXDocument();
                    MergeStyles(sourceDocument, newDocument, oldStyles, newStyles, newContent);
                    MergeLatentStyles(oldStyles, newStyles);
                }
            }

            // Copy all styles with effects to the new document
            if (sourceDocument.MainDocumentPart.StylesWithEffectsPart is not null)
            {
                XDocument oldStyles = sourceDocument.MainDocumentPart.StylesWithEffectsPart.GetXDocument();

                if (newDocument.MainDocumentPart!.StylesWithEffectsPart is null)
                {
                    newDocument.MainDocumentPart.AddNewPart<StylesWithEffectsPart>();
                    XDocument newStyles = newDocument.MainDocumentPart.StylesWithEffectsPart!.GetXDocument();
                    newStyles.Add(oldStyles.Root);
                }
                else
                {
                    XDocument newStyles = newDocument.MainDocumentPart.StylesWithEffectsPart.GetXDocument();
                    MergeStyles(sourceDocument, newDocument, oldStyles, newStyles, newContent);
                    MergeLatentStyles(oldStyles, newStyles);
                }
            }

            // Copy fontTable to the new document
            if (sourceDocument.MainDocumentPart.FontTablePart is not null)
            {
                XDocument oldFontTable = sourceDocument.MainDocumentPart.FontTablePart.GetXDocument();

                if (newDocument.MainDocumentPart!.FontTablePart is null)
                {
                    newDocument.MainDocumentPart.AddNewPart<FontTablePart>();
                    XDocument newFontTable = newDocument.MainDocumentPart.FontTablePart!.GetXDocument();
                    newFontTable.Add(oldFontTable.Root);
                }
                else
                {
                    XDocument newFontTable = newDocument.MainDocumentPart.FontTablePart.GetXDocument();
                    MergeFontTables(oldFontTable, newFontTable);
                }
            }
        }

        private static void CopyComments(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IReadOnlyCollection<XElement> newContent,
            List<ImageData> images)
        {
            if (sourceDocument.MainDocumentPart?.WordprocessingCommentsPart is null || !newContent.Any())
            {
                return;
            }

            var commentIdMap = new Dictionary<int, int>();

            XDocument oldComments = sourceDocument.MainDocumentPart.WordprocessingCommentsPart.GetXDocument();

            XDocument? newComments;
            var number = 0;

            if (newDocument.MainDocumentPart!.WordprocessingCommentsPart is not null)
            {
                newComments = newDocument.MainDocumentPart.WordprocessingCommentsPart.GetXDocument();
                List<int> ids = newComments.Root!.Elements(W.comment).Select(f => (int) f.Attribute(W.id)).ToList();

                if (ids.Any())
                {
                    number = ids.Max() + 1;
                }
            }
            else
            {
                newDocument.MainDocumentPart.AddNewPart<WordprocessingCommentsPart>();
                newComments = newDocument.MainDocumentPart.WordprocessingCommentsPart!.GetXDocument();
                newComments.Add(new XElement(W.comments, NamespaceAttributes));
            }

            foreach (XElement comment in newContent.DescendantsAndSelf(W.commentReference))
            {
                if (!int.TryParse((string) comment.Attribute(W.id), out int id))
                {
                    throw new DocumentBuilderException("Invalid document - invalid comment id");
                }

                XElement? element = oldComments
                    .Root!
                    .Elements(W.comment)
                    .FirstOrDefault(p => int.TryParse((string) p.Attribute(W.id), out int thisId)
                        ? thisId == id
                        : throw new DocumentBuilderException("Invalid document - invalid comment id"));

                if (element is null)
                {
                    throw new DocumentBuilderException(
                        "Invalid document - comment reference without associated comment in comments part");
                }

                var newElement = new XElement(element);
                newElement.SetAttributeValue(W.id, number);
                newComments.Root!.Add(newElement);

                if (!commentIdMap.ContainsKey(id))
                {
                    commentIdMap.Add(id, number);
                }

                number++;
            }

            foreach (XElement item in newContent.DescendantsAndSelf()
                         .Where(d => d.Name == W.commentReference || d.Name == W.commentRangeStart || d.Name == W.commentRangeEnd)
                         .ToList())
            {
                var idVal = (int) item.Attribute(W.id);

                if (commentIdMap.ContainsKey(idVal))
                {
                    item.SetAttributeValue(W.id, commentIdMap[idVal]);
                }
            }

            if (sourceDocument.MainDocumentPart!.WordprocessingCommentsPart is not null &&
                newDocument.MainDocumentPart!.WordprocessingCommentsPart is not null)
            {
                AddRelationships(sourceDocument.MainDocumentPart.WordprocessingCommentsPart,
                    newDocument.MainDocumentPart.WordprocessingCommentsPart,
                    new[] { newDocument.MainDocumentPart.WordprocessingCommentsPart.GetXElement()! });

                CopyRelatedPartsForContentParts(sourceDocument.MainDocumentPart.WordprocessingCommentsPart,
                    newDocument.MainDocumentPart.WordprocessingCommentsPart,
                    new[] { newDocument.MainDocumentPart.WordprocessingCommentsPart.GetXElement()! },
                    images);
            }
        }

        private static void AdjustUniqueIds(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IReadOnlyCollection<XElement> newContent)
        {
            // adjust bookmark unique ids
            var maxId = 0;

            if (newDocument.MainDocumentPart!.GetXDocument().Descendants(W.bookmarkStart).Any())
            {
                maxId = newDocument
                    .MainDocumentPart!
                    .GetXDocument()
                    .Descendants(W.bookmarkStart)
                    .Select(d => (int) d.Attribute(W.id))
                    .Max();
            }

            var bookmarkIdMap = new Dictionary<int, int>();

            foreach (XElement item in newContent
                         .DescendantsAndSelf()
                         .Where(bm => bm.Name == W.bookmarkStart || bm.Name == W.bookmarkEnd))
            {
                if (!int.TryParse((string) item.Attribute(W.id), out int id))
                {
                    throw new DocumentBuilderException("Invalid document - invalid value for bookmark ID");
                }

                if (!bookmarkIdMap.ContainsKey(id))
                {
                    bookmarkIdMap.Add(id, ++maxId);
                }
            }

            foreach (XElement bookmarkElement in newContent
                         .DescendantsAndSelf()
                         .Where(e => e.Name == W.bookmarkStart || e.Name == W.bookmarkEnd))
            {
                bookmarkElement.SetAttributeValue(W.id, bookmarkIdMap[(int) bookmarkElement.Attribute(W.id)]);
            }

            // adjust shape unique ids
            // This doesn't work because OLEObjects refer to shapes by ID.
            // Punting on this, because sooner or later, this will be a non-issue.
            //foreach (var item in newContent.DescendantsAndSelf(VML.shape))
            //{
            //    Guid g = Guid.NewGuid();
            //    string s = "R" + g.ToString().Replace("-", "");
            //    item.Attribute(NoNamespace.id).Value = s;
            //}
        }

        private static void AdjustDocPrIds(WordprocessingDocument newDocument)
        {
            var docPrId = 0;

            foreach (XElement item in newDocument.MainDocumentPart!.GetXDocument().Descendants(WP.docPr))
            {
                item.SetAttributeValue(NoNamespace.id, ++docPrId);
            }

            foreach (HeaderPart header in newDocument.MainDocumentPart!.HeaderParts)
            {
                foreach (XElement item in header.GetXDocument().Descendants(WP.docPr))
                {
                    item.SetAttributeValue(NoNamespace.id, ++docPrId);
                }
            }

            foreach (FooterPart footer in newDocument.MainDocumentPart.FooterParts)
            {
                foreach (XElement item in footer.GetXDocument().Descendants(WP.docPr))
                {
                    item.SetAttributeValue(NoNamespace.id, ++docPrId);
                }
            }

            if (newDocument.MainDocumentPart.FootnotesPart != null)
            {
                foreach (XElement item in newDocument.MainDocumentPart.FootnotesPart.GetXDocument().Descendants(WP.docPr))
                {
                    item.SetAttributeValue(NoNamespace.id, ++docPrId);
                }
            }

            if (newDocument.MainDocumentPart.EndnotesPart != null)
            {
                foreach (XElement item in newDocument.MainDocumentPart.EndnotesPart.GetXDocument().Descendants(WP.docPr))
                {
                    item.SetAttributeValue(NoNamespace.id, ++docPrId);
                }
            }
        }

        // This probably doesn't need to be done, except that the Open XML SDK will not validate
        // documents that contain the o:gfxdata attribute.
        private static void RemoveGfxdata(IEnumerable<XElement> newContent)
        {
            newContent.DescendantsAndSelf().Attributes(O.gfxdata).Remove();
        }

        // Rules for sections
        // - if KeepSections for all documents in the source collection are false, then it takes the section
        //   from the first document.
        // - if you specify true for any document, and if the last section is part of the specified content,
        //   then that section is copied.  If any paragraph in the content has a section, then that section
        //   is copied.
        // - if you specify true for any document, and there are no sections for any paragraphs, then no
        //   sections are copied.
        private static void AppendDocument(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            List<XElement> newContent,
            bool keepSection,
            string? insertId,
            List<ImageData> images)
        {
            Debug.Assert(sourceDocument.MainDocumentPart is not null);
            Debug.Assert(newDocument.MainDocumentPart is not null);

            FixRanges(sourceDocument.MainDocumentPart!.GetXElementOrThrow(), newContent);
            AddRelationships(sourceDocument.MainDocumentPart!, newDocument.MainDocumentPart!, newContent);

            CopyRelatedPartsForContentParts(sourceDocument.MainDocumentPart!, newDocument.MainDocumentPart!,
                newContent, images);

            // Append contents
            XDocument newMainXDoc = newDocument.MainDocumentPart!.GetXDocument();

            if (keepSection == false)
            {
                List<XElement> adjustedContents = newContent.Where(e => e.Name != W.sectPr).ToList();
                adjustedContents.DescendantsAndSelf(W.sectPr).Remove();
                newContent = adjustedContents;
            }

            List<XElement> listOfSectionProps = newContent.DescendantsAndSelf(W.sectPr).ToList();

            foreach (XElement sectPr in listOfSectionProps)
            {
                AddSectionAndDependencies(sourceDocument, newDocument, sectPr, images);
            }

            CopyStylesAndFonts(sourceDocument, newDocument, newContent);
            CopyNumbering(sourceDocument, newDocument, newContent, images);
            CopyComments(sourceDocument, newDocument, newContent, images);
            CopyFootnotes(sourceDocument, newDocument, newContent, images);
            CopyEndnotes(sourceDocument, newDocument, newContent, images);
            AdjustUniqueIds(sourceDocument, newDocument, newContent);
            RemoveGfxdata(newContent);
            CopyCustomXmlPartsForDataBoundContentControls(sourceDocument, newDocument, newContent);
            CopyWebExtensions(sourceDocument, newDocument);

            if (insertId != null)
            {
                XElement? insertElementToReplace = newMainXDoc
                    .Descendants(PtOpenXml.Insert)
                    .FirstOrDefault(i => (string) i.Attribute(PtOpenXml.Id) == insertId);

                insertElementToReplace?.AddAnnotation(new ReplaceSemaphore());

                newMainXDoc.Root!.ReplaceWith((XElement) InsertTransform(newMainXDoc.Root, newContent));
            }
            else
            {
                newMainXDoc.Root!.Element(W.body)!.Add(newContent);
            }

            if (newMainXDoc
                .Descendants()
                .Any(d =>
                    d.Name.Namespace == PtOpenXml.pt ||
                    d.Name.Namespace == PtOpenXml.ptOpenXml ||
                    d.Attributes().Any(a => a.Name.Namespace == PtOpenXml.pt || a.Name.Namespace == PtOpenXml.ptOpenXml)))
            {
                XElement root = newMainXDoc.Root;

                if (root.Attributes().All(na => na.Value != PtOpenXml.pt.NamespaceName))
                {
                    root.Add(new XAttribute(XNamespace.Xmlns + "pt", PtOpenXml.pt.NamespaceName));
                    AddToIgnorable(root, "pt");
                }

                if (root.Attributes().All(na => na.Value != PtOpenXml.ptOpenXml.NamespaceName))
                {
                    root.Add(new XAttribute(XNamespace.Xmlns + "pt14", PtOpenXml.ptOpenXml.NamespaceName));
                    AddToIgnorable(root, "pt14");
                }
            }
        }

        private static object InsertTransform(XNode node, List<XElement> newContent)
        {
            if (node is XElement element)
            {
                if (element.Annotation<ReplaceSemaphore>() != null)
                {
                    return newContent;
                }

                return new XElement(element.Name,
                    element.Attributes(),
                    element.Nodes().Select(n => InsertTransform(n, newContent)));
            }

            return node;
        }

        private static void CopyWebExtensions(WordprocessingDocument sourceDocument, WordprocessingDocument newDocument)
        {
            if (sourceDocument.WebExTaskpanesPart == null || newDocument.WebExTaskpanesPart != null)
            {
                return;
            }

            WebExTaskpanesPart part = newDocument.AddWebExTaskpanesPart();
            part.GetXDocument().Add(sourceDocument.WebExTaskpanesPart.GetXElement());

            foreach (WebExtensionPart sourceWebExtensionPart in sourceDocument.WebExTaskpanesPart.WebExtensionParts)
            {
                var newWebExtensionpart = part.AddNewPart<WebExtensionPart>(
                    sourceDocument.WebExTaskpanesPart.GetIdOfPart(sourceWebExtensionPart));

                newWebExtensionpart.GetXDocument().Add(sourceWebExtensionPart.GetXElement());
            }
        }

        private static void AddToIgnorable(XElement root, string v)
        {
            XAttribute? ignorable = root.Attribute(MC.Ignorable);

            if (ignorable == null)
            {
                return;
            }

            var val = (string) ignorable;
            val = val + " " + v;
            ignorable.Remove();
            root.SetAttributeValue(MC.Ignorable, val);
        }

        // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // New method to support new functionality
        private static void AppendDocument(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            OpenXmlPart part,
            List<XElement> newContent,
            bool keepSection,
            string insertId,
            List<ImageData> images)
        {
            XElement partRootXElement = part.GetXElementOrThrow();
            FixRanges(partRootXElement, newContent);

            MainDocumentPart sourceMainDocumentPart = sourceDocument.MainDocumentPart!;
            AddRelationships(sourceMainDocumentPart, part, newContent);
            CopyRelatedPartsForContentParts(sourceMainDocumentPart, part, newContent, images);

            // never keep sections for content to be inserted into a header/footer
            List<XElement> adjustedContents = newContent.Where(e => e.Name != W.sectPr).ToList();
            adjustedContents.DescendantsAndSelf(W.sectPr).Remove();
            newContent = adjustedContents;

            CopyNumbering(sourceDocument, newDocument, newContent, images);
            CopyComments(sourceDocument, newDocument, newContent, images);
            AdjustUniqueIds(sourceDocument, newDocument, newContent);
            RemoveGfxdata(newContent);

            if (insertId == null)
            {
                throw new OpenXmlPowerToolsException("Internal error");
            }

            XElement? insertElementToReplace = partRootXElement
                .Descendants(PtOpenXml.Insert)
                .FirstOrDefault(i => (string) i.Attribute(PtOpenXml.Id) == insertId);

            insertElementToReplace?.AddAnnotation(new ReplaceSemaphore());

            partRootXElement.ReplaceWith((XElement) InsertTransform(partRootXElement, newContent));
        }

        // ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        private static WmlDocument? ExtractGlossaryDocument(WmlDocument wmlGlossaryDocument)
        {
            using var ms = new MemoryStream();

            ms.Write(wmlGlossaryDocument.DocumentByteArray, 0, wmlGlossaryDocument.DocumentByteArray.Length);

            using WordprocessingDocument wDoc = WordprocessingDocument.Open(ms, false);

            XDocument? fromXd = wDoc.MainDocumentPart?.GlossaryDocumentPart?.GetXDocument();

            if (fromXd?.Root == null)
            {
                return null;
            }

            using var outMs = new MemoryStream();

            using (var outWDoc = WordprocessingDocument.Create(outMs, WordprocessingDocumentType.Document))
            {
                var images = new List<ImageData>();

                MainDocumentPart mdp = outWDoc.AddMainDocumentPart();
                XDocument mdpXd = mdp.GetXDocument();
                var root = new XElement(W.document);

                if (mdpXd.Root == null)
                {
                    mdpXd.Add(root);
                }
                else
                {
                    mdpXd.Root.ReplaceWith(root);
                }

                root.Add(new XElement(W.body, fromXd.Root.Elements(W.docParts)));
                mdp.SaveXDocument();

                List<XElement> newContent = fromXd.Root.Elements(W.docParts).ToList();
                CopyGlossaryDocumentPartsFromGd(wDoc, outWDoc, newContent, images);
                CopyRelatedPartsForContentParts(wDoc.MainDocumentPart!.GlossaryDocumentPart!, mdp, newContent, images);
            }

            return new WmlDocument("Glossary.docx", outMs.ToArray());
        }

        private static void CopyGlossaryDocumentPartsFromGd(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IReadOnlyCollection<XElement> newContent,
            List<ImageData> images)
        {
            GlossaryDocumentPart? sourceGlossaryDocumentPart = sourceDocument.MainDocumentPart?.GlossaryDocumentPart;

            if (sourceGlossaryDocumentPart is null)
            {
                return;
            }

            // Copy all styles to the new document
            if (sourceGlossaryDocumentPart.StyleDefinitionsPart is not null)
            {
                XDocument oldStyles = sourceGlossaryDocumentPart.StyleDefinitionsPart.GetXDocument();

                if (newDocument.MainDocumentPart!.StyleDefinitionsPart is null)
                {
                    var styleDefinitionsPart = newDocument.MainDocumentPart.AddNewPart<StyleDefinitionsPart>();
                    styleDefinitionsPart.SetXDocument(oldStyles);
                }
                else
                {
                    XDocument newStyles = newDocument.MainDocumentPart.StyleDefinitionsPart.GetXDocument();
                    MergeStyles(sourceDocument, newDocument, oldStyles, newStyles, newContent);
                    newDocument.MainDocumentPart.StyleDefinitionsPart.SaveXDocument();
                }
            }

            // Copy fontTable to the new document
            if (sourceGlossaryDocumentPart.FontTablePart is not null)
            {
                XDocument oldFontTable = sourceGlossaryDocumentPart.FontTablePart.GetXDocument();

                if (newDocument.MainDocumentPart!.FontTablePart is null)
                {
                    var fontTablePart = newDocument.MainDocumentPart.AddNewPart<FontTablePart>();
                    fontTablePart.SetXDocument(oldFontTable);
                }
                else
                {
                    XDocument newFontTable = newDocument.MainDocumentPart.FontTablePart.GetXDocument();
                    MergeFontTables(oldFontTable, newFontTable);
                    newDocument.MainDocumentPart.FontTablePart.SaveXDocument();
                }
            }

            DocumentSettingsPart? oldSettingsPart = sourceGlossaryDocumentPart.DocumentSettingsPart;

            if (oldSettingsPart != null)
            {
                var newSettingsPart = newDocument.MainDocumentPart!.AddNewPart<DocumentSettingsPart>();
                XDocument settingsXDoc = oldSettingsPart.GetXDocument();
                AddRelationships(oldSettingsPart, newSettingsPart, new[] { settingsXDoc.Root });
                //CopyFootnotesPart(sourceDocument, newDocument, settingsXDoc, images);
                //CopyEndnotesPart(sourceDocument, newDocument, settingsXDoc, images);

                XDocument newXDoc = newSettingsPart.GetXDocument();
                newXDoc.Add(settingsXDoc.Root);
                CopyRelatedPartsForContentParts(oldSettingsPart, newSettingsPart, new[] { newXDoc.Root }, images);
                newSettingsPart.SetXDocument(newXDoc);
            }

            WebSettingsPart? oldWebSettingsPart = sourceGlossaryDocumentPart.WebSettingsPart;

            if (oldWebSettingsPart != null)
            {
                var newWebSettingsPart = newDocument.MainDocumentPart!.AddNewPart<WebSettingsPart>();
                XDocument settingsXDoc = oldWebSettingsPart.GetXDocument();
                AddRelationships(oldWebSettingsPart, newWebSettingsPart, new[] { settingsXDoc.Root });

                XDocument newXDoc = newWebSettingsPart.GetXDocument();
                newXDoc.Add(settingsXDoc.Root);
                newWebSettingsPart.SetXDocument(newXDoc);
            }

            NumberingDefinitionsPart? oldNumberingDefinitionsPart = sourceGlossaryDocumentPart.NumberingDefinitionsPart;

            if (oldNumberingDefinitionsPart != null)
            {
                CopyNumberingForGlossaryDocumentPartFromGD(oldNumberingDefinitionsPart, newDocument, newContent, images);
            }
        }

        private static void CopyGlossaryDocumentPartsToGd(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IReadOnlyCollection<XElement> newContent,
            List<ImageData> images)
        {
            GlossaryDocumentPart newGlossaryDocumentPart = newDocument.MainDocumentPart!.GlossaryDocumentPart!;

            // Copy all styles to the new document
            if (sourceDocument.MainDocumentPart!.StyleDefinitionsPart is not null)
            {
                XDocument oldStyles = sourceDocument.MainDocumentPart.StyleDefinitionsPart.GetXDocument();
                newGlossaryDocumentPart.AddNewPart<StyleDefinitionsPart>();
                XDocument newStyles = newGlossaryDocumentPart.StyleDefinitionsPart!.GetXDocument();
                newStyles.Add(oldStyles.Root);
                newGlossaryDocumentPart.StyleDefinitionsPart!.SaveXDocument();
            }

            // Copy fontTable to the new document
            if (sourceDocument.MainDocumentPart.FontTablePart is not null)
            {
                XDocument oldFontTable = sourceDocument.MainDocumentPart.FontTablePart.GetXDocument();
                newGlossaryDocumentPart.AddNewPart<FontTablePart>();
                XDocument newFontTable = newGlossaryDocumentPart.FontTablePart!.GetXDocument();
                newFontTable.Add(oldFontTable.Root);
                newDocument.MainDocumentPart.FontTablePart!.SaveXDocument();
            }

            DocumentSettingsPart? oldSettingsPart = sourceDocument.MainDocumentPart.DocumentSettingsPart;

            if (oldSettingsPart is not null)
            {
                var newSettingsPart = newGlossaryDocumentPart.AddNewPart<DocumentSettingsPart>();
                XDocument settingsXDoc = oldSettingsPart.GetXDocument();
                AddRelationships(oldSettingsPart, newSettingsPart, new[] { settingsXDoc.Root });
                //CopyFootnotesPart(sourceDocument, newDocument, settingsXDoc, images);
                //CopyEndnotesPart(sourceDocument, newDocument, settingsXDoc, images);
                XDocument newXDoc = newGlossaryDocumentPart.DocumentSettingsPart!.GetXDocument();
                newXDoc.Add(settingsXDoc.Root);
                CopyRelatedPartsForContentParts(oldSettingsPart, newSettingsPart, new[] { newXDoc.Root }, images);
                newSettingsPart.SetXDocument(newXDoc);
            }

            WebSettingsPart? oldWebSettingsPart = sourceDocument.MainDocumentPart.WebSettingsPart;

            if (oldWebSettingsPart is not null)
            {
                var newWebSettingsPart = newGlossaryDocumentPart.AddNewPart<WebSettingsPart>();
                XDocument settingsXDoc = oldWebSettingsPart.GetXDocument();
                AddRelationships(oldWebSettingsPart, newWebSettingsPart, new[] { settingsXDoc.Root });
                XDocument newXDoc = newGlossaryDocumentPart.WebSettingsPart!.GetXDocument();
                newXDoc.Add(settingsXDoc.Root);
                newWebSettingsPart.SetXDocument(newXDoc);
            }

            NumberingDefinitionsPart? oldNumberingDefinitionsPart = sourceDocument.MainDocumentPart.NumberingDefinitionsPart;

            if (oldNumberingDefinitionsPart is not null)
            {
                CopyNumberingForGlossaryDocumentPartToGD(oldNumberingDefinitionsPart, newDocument, newContent, images);
            }
        }

#if false
        At various locations in Open-Xml-PowerTools, you will find examples of Open XML markup that is associated with code associated with
        querying or generating that markup.  This is an example of the GlossaryDocument part.

<w:glossaryDocument xmlns:wpc = "http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas" xmlns:cx =
 "http://schemas.microsoft.com/office/drawing/2014/chartex" xmlns:cx1 =
 "http://schemas.microsoft.com/office/drawing/2015/9/8/chartex" xmlns:cx2 =
 "http://schemas.microsoft.com/office/drawing/2015/10/21/chartex" xmlns:cx3 =
 "http://schemas.microsoft.com/office/drawing/2016/5/9/chartex" xmlns:cx4 =
 "http://schemas.microsoft.com/office/drawing/2016/5/10/chartex" xmlns:cx5 =
 "http://schemas.microsoft.com/office/drawing/2016/5/11/chartex" xmlns:cx6 =
 "http://schemas.microsoft.com/office/drawing/2016/5/12/chartex" xmlns:cx7 =
 "http://schemas.microsoft.com/office/drawing/2016/5/13/chartex" xmlns:cx8 =
 "http://schemas.microsoft.com/office/drawing/2016/5/14/chartex" xmlns:mc =
 "http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:aink =
 "http://schemas.microsoft.com/office/drawing/2016/ink" xmlns:am3d =
 "http://schemas.microsoft.com/office/drawing/2017/model3d" xmlns:o = "urn:schemas-microsoft-com:office:office" xmlns:r =
 "http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:m =
 "http://schemas.openxmlformats.org/officeDocument/2006/math" xmlns:v = "urn:schemas-microsoft-com:vml" xmlns:wp14 =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing" xmlns:wp =
 "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" xmlns:w10 =
 "urn:schemas-microsoft-com:office:word" xmlns:w = "http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:w14 =
 "http://schemas.microsoft.com/office/word/2010/wordml" xmlns:w15 =
 "http://schemas.microsoft.com/office/word/2012/wordml" xmlns:w16cid =
 "http://schemas.microsoft.com/office/word/2016/wordml/cid" xmlns:w16se =
 "http://schemas.microsoft.com/office/word/2015/wordml/symex" xmlns:wpg =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingGroup" xmlns:wpi =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingInk" xmlns:wne =
 "http://schemas.microsoft.com/office/word/2006/wordml" xmlns:wps =
 "http://schemas.microsoft.com/office/word/2010/wordprocessingShape" mc:Ignorable = "w14 w15 w16se w16cid wp14">
  <w:docParts>
    <w:docPart>
      <w:docPartPr>
        <w:name w:val = "CDE7B64C7BB446AE905B622B0A882EB6" />
        <w:category>
          <w:name w:val = "General" />
          <w:gallery w:val = "placeholder" />
        </w:category>
        <w:types>
          <w:type w:val = "bbPlcHdr" />
        </w:types>
        <w:behaviors>
          <w:behavior w:val = "content" />
        </w:behaviors>
        <w:guid w:val = "{13882A71-B5B7-4421-ACBB-9B61C61B3034}" />
      </w:docPartPr>
      <w:docPartBody>
        <w:p w:rsidR = "00004EEA" w:rsidRDefault = "00AD57F5" w:rsidP = "00AD57F5">
          <w:pPr>
            <w:pStyle w:val = "CDE7B64C7BB446AE905B622B0A882EB6" />
          </w:pPr>
          <w:r w:rsidRPr = "00FB619D">
            <w:rPr>
              <w:rStyle w:val = "PlaceholderText" />
              <w:lang w:val = "da-DK" />
            </w:rPr>
            <w:t>Produktnavn</w:t>
          </w:r>
          <w:r w:rsidRPr = "007379EE">
            <w:rPr>
              <w:rStyle w:val = "PlaceholderText" />
            </w:rPr>
            <w:t>.</w:t>
          </w:r>
        </w:p>
      </w:docPartBody>
    </w:docPart>
  </w:docParts>
</w:glossaryDocument>
#endif

        private static void CopyCustomXmlPartsForDataBoundContentControls(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent)
        {
            var itemList = new List<string>();

            foreach (string itemId in newContent
                         .Descendants(W.dataBinding)
                         .Select(e => (string) e.Attribute(W.storeItemID)))
            {
                if (!itemList.Contains(itemId))
                {
                    itemList.Add(itemId);
                }
            }

            foreach (CustomXmlPart customXmlPart in sourceDocument.MainDocumentPart!.CustomXmlParts)
            {
                OpenXmlPart? propertyPart = customXmlPart
                    .Parts
                    .Select(p => p.OpenXmlPart)
                    .FirstOrDefault(p => p.ContentType == "application/vnd.openxmlformats-officedocument.customXmlProperties+xml");

                if (propertyPart != null)
                {
                    XDocument propertyPartDoc = propertyPart.GetXDocument();

                    if (itemList.Contains(propertyPartDoc.Root.Attribute(DS.itemID).Value))
                    {
                        CustomXmlPart newPart = newDocument.MainDocumentPart.AddCustomXmlPart(customXmlPart.ContentType);
                        newPart.GetXDocument().Add(customXmlPart.GetXElement());

                        foreach (OpenXmlPart propPart in customXmlPart.Parts.Select(p => p.OpenXmlPart))
                        {
                            var newPropPart = newPart.AddNewPart<CustomXmlPropertiesPart>();
                            newPropPart.GetXDocument().Add(propPart.GetXElement());
                        }
                    }
                }
            }
        }

        private static void UpdateContent(IEnumerable<XElement> newContent, XName elementToModify, string oldRid, string newRid)
        {
            foreach (XName attributeName in RelationshipMarkup[elementToModify])
            {
                IEnumerable<XElement> elementsToUpdate = newContent
                    .Descendants(elementToModify)
                    .Where(e => (string) e.Attribute(attributeName) == oldRid);

                foreach (XElement element in elementsToUpdate)
                {
                    element.Attribute(attributeName).Value = newRid;
                }
            }
        }

        private static void AddRelationships(OpenXmlPart oldPart, OpenXmlPart newPart, IEnumerable<XElement> newContent)
        {
            IEnumerable<XElement> relevantElements = newContent.DescendantsAndSelf()
                .Where(d => RelationshipMarkup.ContainsKey(d.Name) &&
                    d.Attributes().Any(a => RelationshipMarkup[d.Name].Contains(a.Name)));

            foreach (XElement e in relevantElements)
            {
                if (e.Name == W.hyperlink)
                {
                    var relId = (string) e.Attribute(R.id);

                    if (string.IsNullOrEmpty(relId))
                    {
                        continue;
                    }

                    HyperlinkRelationship tempHyperlink = newPart.HyperlinkRelationships.FirstOrDefault(h => h.Id == relId);

                    if (tempHyperlink != null)
                    {
                        continue;
                    }

                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    HyperlinkRelationship oldHyperlink = oldPart.HyperlinkRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldHyperlink == null)
                    {
                        continue;
                    }

                    //throw new DocumentBuilderInternalException("Internal Error 0002");
                    newPart.AddHyperlinkRelationship(oldHyperlink.Uri, oldHyperlink.IsExternal, newRid);
                    UpdateContent(newContent, e.Name, relId, newRid);
                }

                if (e.Name == W.attachedTemplate || e.Name == W.saveThroughXslt)
                {
                    var relId = (string) e.Attribute(R.id);

                    if (string.IsNullOrEmpty(relId))
                    {
                        continue;
                    }

                    ExternalRelationship tempExternalRelationship =
                        newPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (tempExternalRelationship != null)
                    {
                        continue;
                    }

                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    ExternalRelationship oldRel = oldPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldRel == null)
                    {
                        throw new DocumentBuilderInternalException(
                            "Source {0} is invalid document - hyperlink contains invalid references");
                    }

                    newPart.AddExternalRelationship(oldRel.RelationshipType, oldRel.Uri, newRid);
                    UpdateContent(newContent, e.Name, relId, newRid);
                }

                if (e.Name == A.hlinkClick || e.Name == A.hlinkHover || e.Name == A.hlinkMouseOver)
                {
                    var relId = (string) e.Attribute(R.id);

                    if (string.IsNullOrEmpty(relId))
                    {
                        continue;
                    }

                    HyperlinkRelationship tempHyperlink = newPart.HyperlinkRelationships.FirstOrDefault(h => h.Id == relId);

                    if (tempHyperlink != null)
                    {
                        continue;
                    }

                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    HyperlinkRelationship oldHyperlink = oldPart.HyperlinkRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldHyperlink == null)
                    {
                        continue;
                    }

                    newPart.AddHyperlinkRelationship(oldHyperlink.Uri, oldHyperlink.IsExternal, newRid);
                    UpdateContent(newContent, e.Name, relId, newRid);
                }

                if (e.Name == VML.imagedata)
                {
                    var relId = (string) e.Attribute(R.href);

                    if (string.IsNullOrEmpty(relId))
                    {
                        continue;
                    }

                    ExternalRelationship tempExternalRelationship =
                        newPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (tempExternalRelationship != null)
                    {
                        continue;
                    }

                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    ExternalRelationship oldRel = oldPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldRel == null)
                    {
                        throw new DocumentBuilderInternalException("Internal Error 0006");
                    }

                    newPart.AddExternalRelationship(oldRel.RelationshipType, oldRel.Uri, newRid);
                    UpdateContent(newContent, e.Name, relId, newRid);
                }

                if (e.Name == A.blip)
                {
                    // <a:blip r:embed="rId6" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" />
                    var relId = (string) e.Attribute(R.link);

                    //if (relId == null)
                    //    relId = (string)e.Attribute(R.embed);
                    if (string.IsNullOrEmpty(relId))
                    {
                        continue;
                    }

                    ExternalRelationship tempExternalRelationship =
                        newPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (tempExternalRelationship != null)
                    {
                        continue;
                    }

                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    ExternalRelationship oldRel = oldPart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldRel == null)
                    {
                        continue;
                    }

                    newPart.AddExternalRelationship(oldRel.RelationshipType, oldRel.Uri, newRid);
                    UpdateContent(newContent, e.Name, relId, newRid);
                }
            }
        }

        private class FromPreviousSourceSemaphore
        {
        }

        private static void CopyNumbering(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent,
            List<ImageData> images)
        {
            var numIdMap = new Dictionary<int, int>();
            var number = 1;
            var abstractNumber = 0;
            XDocument oldNumbering = null;
            XDocument newNumbering = null;

            foreach (XElement numReference in newContent.DescendantsAndSelf(W.numPr))
            {
                XElement idElement = numReference.Descendants(W.numId).FirstOrDefault();

                if (idElement != null)
                {
                    if (oldNumbering == null)
                    {
                        oldNumbering = sourceDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();
                    }

                    if (newNumbering == null)
                    {
                        if (newDocument.MainDocumentPart.NumberingDefinitionsPart != null)
                        {
                            newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();

                            IEnumerable<int> numIds = newNumbering
                                .Root
                                .Elements(W.num)
                                .Select(f => (int) f.Attribute(W.numId));

                            if (numIds.Any())
                            {
                                number = numIds.Max() + 1;
                            }

                            numIds = newNumbering
                                .Root
                                .Elements(W.abstractNum)
                                .Select(f => (int) f.Attribute(W.abstractNumId));

                            if (numIds.Any())
                            {
                                abstractNumber = numIds.Max() + 1;
                            }
                        }
                        else
                        {
                            newDocument.MainDocumentPart.AddNewPart<NumberingDefinitionsPart>();
                            newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();
                            newNumbering.Declaration.Standalone = Yes;
                            newNumbering.Declaration.Encoding = Utf8;
                            newNumbering.Add(new XElement(W.numbering, NamespaceAttributes));
                        }
                    }

                    var numId = (int) idElement.Attribute(W.val);

                    if (numId != 0)
                    {
                        XElement element = oldNumbering
                            .Descendants(W.num)
                            .Where(p => (int) p.Attribute(W.numId) == numId)
                            .FirstOrDefault();

                        if (element == null)
                        {
                            continue;
                        }

                        // Copy abstract numbering element, if necessary (use matching NSID)
                        var abstractNumIdStr = (string) element
                            .Elements(W.abstractNumId)
                            .First()
                            .Attribute(W.val);

                        int abstractNumId;

                        if (!int.TryParse(abstractNumIdStr, out abstractNumId))
                        {
                            throw new DocumentBuilderException("Invalid document - invalid value for abstractNumId");
                        }

                        XElement abstractElement = oldNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(p => (int) p.Attribute(W.abstractNumId) == abstractNumId)
                            .First();

                        XElement nsidElement = abstractElement
                            .Element(W.nsid);

                        string abstractNSID = null;

                        if (nsidElement != null)
                        {
                            abstractNSID = (string) nsidElement
                                .Attribute(W.val);
                        }

                        XElement newAbstractElement = newNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                            .Where(p =>
                            {
                                XElement thisNsidElement = p.Element(W.nsid);

                                if (thisNsidElement == null)
                                {
                                    return false;
                                }

                                return (string) thisNsidElement.Attribute(W.val) == abstractNSID;
                            })
                            .FirstOrDefault();

                        if (newAbstractElement == null)
                        {
                            newAbstractElement = new XElement(abstractElement);
                            newAbstractElement.Attribute(W.abstractNumId).Value = abstractNumber.ToString();
                            abstractNumber++;

                            if (newNumbering.Root.Elements(W.abstractNum).Any())
                            {
                                newNumbering.Root.Elements(W.abstractNum).Last().AddAfterSelf(newAbstractElement);
                            }
                            else
                            {
                                newNumbering.Root.Add(newAbstractElement);
                            }

                            foreach (XElement pictId in newAbstractElement.Descendants(W.lvlPicBulletId))
                            {
                                var bulletId = (string) pictId.Attribute(W.val);

                                XElement numPicBullet = oldNumbering
                                    .Descendants(W.numPicBullet)
                                    .FirstOrDefault(d => (string) d.Attribute(W.numPicBulletId) == bulletId);

                                int maxNumPicBulletId = new[] { -1 }.Concat(newNumbering.Descendants(W.numPicBullet)
                                            .Attributes(W.numPicBulletId)
                                            .Select(a => (int) a))
                                        .Max() +
                                    1;

                                var newNumPicBullet = new XElement(numPicBullet);
                                newNumPicBullet.Attribute(W.numPicBulletId).Value = maxNumPicBulletId.ToString();
                                pictId.Attribute(W.val).Value = maxNumPicBulletId.ToString();
                                newNumbering.Root.AddFirst(newNumPicBullet);
                            }
                        }

                        string newAbstractId = newAbstractElement.Attribute(W.abstractNumId).Value;

                        // Copy numbering element, if necessary (use matching element with no overrides)
                        XElement newElement;

                        if (numIdMap.ContainsKey(numId))
                        {
                            newElement = newNumbering
                                .Descendants()
                                .Elements(W.num)
                                .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                                .Where(p => (int) p.Attribute(W.numId) == numIdMap[numId])
                                .First();
                        }
                        else
                        {
                            newElement = new XElement(element);

                            newElement
                                .Elements(W.abstractNumId)
                                .First()
                                .Attribute(W.val)
                                .Value = newAbstractId;

                            newElement.Attribute(W.numId).Value = number.ToString();
                            numIdMap.Add(numId, number);
                            number++;
                            newNumbering.Root.Add(newElement);
                        }

                        idElement.Attribute(W.val).Value = newElement.Attribute(W.numId).Value;
                    }
                }
            }

            if (newNumbering != null)
            {
                foreach (XElement abstractNum in newNumbering.Descendants(W.abstractNum))
                {
                    abstractNum.AddAnnotation(new FromPreviousSourceSemaphore());
                }

                foreach (XElement num in newNumbering.Descendants(W.num))
                {
                    num.AddAnnotation(new FromPreviousSourceSemaphore());
                }
            }

            if (newDocument.MainDocumentPart.NumberingDefinitionsPart != null &&
                sourceDocument.MainDocumentPart.NumberingDefinitionsPart != null)
            {
                AddRelationships(sourceDocument.MainDocumentPart.NumberingDefinitionsPart,
                    newDocument.MainDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXElement() });

                CopyRelatedPartsForContentParts(sourceDocument.MainDocumentPart.NumberingDefinitionsPart,
                    newDocument.MainDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXElement() }, images);
            }
        }

        // Note: the following two methods were added with almost exact duplicate code to the method above, because I do not want to touch that code.
        private static void CopyNumberingForGlossaryDocumentPartFromGD(
            NumberingDefinitionsPart sourceNumberingPart,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent,
            List<ImageData> images)
        {
            var numIdMap = new Dictionary<int, int>();
            var number = 1;
            var abstractNumber = 0;
            XDocument oldNumbering = null;
            XDocument newNumbering = null;

            foreach (XElement numReference in newContent.DescendantsAndSelf(W.numPr))
            {
                XElement idElement = numReference.Descendants(W.numId).FirstOrDefault();

                if (idElement != null)
                {
                    if (oldNumbering == null)
                    {
                        oldNumbering = sourceNumberingPart.GetXDocument();
                    }

                    if (newNumbering == null)
                    {
                        if (newDocument.MainDocumentPart.NumberingDefinitionsPart != null)
                        {
                            newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();

                            IEnumerable<int> numIds = newNumbering
                                .Root
                                .Elements(W.num)
                                .Select(f => (int) f.Attribute(W.numId));

                            if (numIds.Any())
                            {
                                number = numIds.Max() + 1;
                            }

                            numIds = newNumbering
                                .Root
                                .Elements(W.abstractNum)
                                .Select(f => (int) f.Attribute(W.abstractNumId));

                            if (numIds.Any())
                            {
                                abstractNumber = numIds.Max() + 1;
                            }
                        }
                        else
                        {
                            newDocument.MainDocumentPart.AddNewPart<NumberingDefinitionsPart>();
                            newNumbering = newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXDocument();
                            newNumbering.Declaration.Standalone = Yes;
                            newNumbering.Declaration.Encoding = Utf8;
                            newNumbering.Add(new XElement(W.numbering, NamespaceAttributes));
                        }
                    }

                    var numId = (int) idElement.Attribute(W.val);

                    if (numId != 0)
                    {
                        XElement element = oldNumbering
                            .Descendants(W.num)
                            .Where(p => (int) p.Attribute(W.numId) == numId)
                            .FirstOrDefault();

                        if (element == null)
                        {
                            continue;
                        }

                        // Copy abstract numbering element, if necessary (use matching NSID)
                        var abstractNumIdStr = (string) element
                            .Elements(W.abstractNumId)
                            .First()
                            .Attribute(W.val);

                        int abstractNumId;

                        if (!int.TryParse(abstractNumIdStr, out abstractNumId))
                        {
                            throw new DocumentBuilderException("Invalid document - invalid value for abstractNumId");
                        }

                        XElement abstractElement = oldNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(p => (int) p.Attribute(W.abstractNumId) == abstractNumId)
                            .First();

                        XElement nsidElement = abstractElement
                            .Element(W.nsid);

                        string abstractNSID = null;

                        if (nsidElement != null)
                        {
                            abstractNSID = (string) nsidElement
                                .Attribute(W.val);
                        }

                        XElement newAbstractElement = newNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                            .Where(p =>
                            {
                                XElement thisNsidElement = p.Element(W.nsid);

                                if (thisNsidElement == null)
                                {
                                    return false;
                                }

                                return (string) thisNsidElement.Attribute(W.val) == abstractNSID;
                            })
                            .FirstOrDefault();

                        if (newAbstractElement == null)
                        {
                            newAbstractElement = new XElement(abstractElement);
                            newAbstractElement.Attribute(W.abstractNumId).Value = abstractNumber.ToString();
                            abstractNumber++;

                            if (newNumbering.Root.Elements(W.abstractNum).Any())
                            {
                                newNumbering.Root.Elements(W.abstractNum).Last().AddAfterSelf(newAbstractElement);
                            }
                            else
                            {
                                newNumbering.Root.Add(newAbstractElement);
                            }

                            foreach (XElement pictId in newAbstractElement.Descendants(W.lvlPicBulletId))
                            {
                                var bulletId = (string) pictId.Attribute(W.val);

                                XElement numPicBullet = oldNumbering
                                    .Descendants(W.numPicBullet)
                                    .FirstOrDefault(d => (string) d.Attribute(W.numPicBulletId) == bulletId);

                                int maxNumPicBulletId = new[] { -1 }.Concat(newNumbering.Descendants(W.numPicBullet)
                                            .Attributes(W.numPicBulletId)
                                            .Select(a => (int) a))
                                        .Max() +
                                    1;

                                var newNumPicBullet = new XElement(numPicBullet);
                                newNumPicBullet.Attribute(W.numPicBulletId).Value = maxNumPicBulletId.ToString();
                                pictId.Attribute(W.val).Value = maxNumPicBulletId.ToString();
                                newNumbering.Root.AddFirst(newNumPicBullet);
                            }
                        }

                        string newAbstractId = newAbstractElement.Attribute(W.abstractNumId).Value;

                        // Copy numbering element, if necessary (use matching element with no overrides)
                        XElement newElement;

                        if (numIdMap.ContainsKey(numId))
                        {
                            newElement = newNumbering
                                .Descendants()
                                .Elements(W.num)
                                .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                                .Where(p => (int) p.Attribute(W.numId) == numIdMap[numId])
                                .First();
                        }
                        else
                        {
                            newElement = new XElement(element);

                            newElement
                                .Elements(W.abstractNumId)
                                .First()
                                .Attribute(W.val)
                                .Value = newAbstractId;

                            newElement.Attribute(W.numId).Value = number.ToString();
                            numIdMap.Add(numId, number);
                            number++;
                            newNumbering.Root.Add(newElement);
                        }

                        idElement.Attribute(W.val).Value = newElement.Attribute(W.numId).Value;
                    }
                }
            }

            if (newNumbering != null)
            {
                foreach (XElement abstractNum in newNumbering.Descendants(W.abstractNum))
                {
                    abstractNum.AddAnnotation(new FromPreviousSourceSemaphore());
                }

                foreach (XElement num in newNumbering.Descendants(W.num))
                {
                    num.AddAnnotation(new FromPreviousSourceSemaphore());
                }
            }

            if (newDocument.MainDocumentPart.NumberingDefinitionsPart != null &&
                sourceNumberingPart != null)
            {
                AddRelationships(sourceNumberingPart,
                    newDocument.MainDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXElement() });

                CopyRelatedPartsForContentParts(sourceNumberingPart,
                    newDocument.MainDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.NumberingDefinitionsPart.GetXElement() }, images);
            }

            if (newDocument.MainDocumentPart.NumberingDefinitionsPart != null)
            {
                newDocument.MainDocumentPart.NumberingDefinitionsPart.SaveXDocument();
            }
        }

        private static void CopyNumberingForGlossaryDocumentPartToGD(
            NumberingDefinitionsPart sourceNumberingPart,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent,
            List<ImageData> images)
        {
            var numIdMap = new Dictionary<int, int>();
            var number = 1;
            var abstractNumber = 0;
            XDocument oldNumbering = null;
            XDocument newNumbering = null;

            foreach (XElement numReference in newContent.DescendantsAndSelf(W.numPr))
            {
                XElement idElement = numReference.Descendants(W.numId).FirstOrDefault();

                if (idElement != null)
                {
                    if (oldNumbering == null)
                    {
                        oldNumbering = sourceNumberingPart.GetXDocument();
                    }

                    if (newNumbering == null)
                    {
                        if (newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart != null)
                        {
                            newNumbering =
                                newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart.GetXDocument();

                            IEnumerable<int> numIds = newNumbering
                                .Root
                                .Elements(W.num)
                                .Select(f => (int) f.Attribute(W.numId));

                            if (numIds.Any())
                            {
                                number = numIds.Max() + 1;
                            }

                            numIds = newNumbering
                                .Root
                                .Elements(W.abstractNum)
                                .Select(f => (int) f.Attribute(W.abstractNumId));

                            if (numIds.Any())
                            {
                                abstractNumber = numIds.Max() + 1;
                            }
                        }
                        else
                        {
                            newDocument.MainDocumentPart.GlossaryDocumentPart.AddNewPart<NumberingDefinitionsPart>();

                            newNumbering =
                                newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart.GetXDocument();

                            newNumbering.Declaration.Standalone = Yes;
                            newNumbering.Declaration.Encoding = Utf8;
                            newNumbering.Add(new XElement(W.numbering, NamespaceAttributes));
                        }
                    }

                    var numId = (int) idElement.Attribute(W.val);

                    if (numId != 0)
                    {
                        XElement element = oldNumbering
                            .Descendants(W.num)
                            .Where(p => (int) p.Attribute(W.numId) == numId)
                            .FirstOrDefault();

                        if (element == null)
                        {
                            continue;
                        }

                        // Copy abstract numbering element, if necessary (use matching NSID)
                        var abstractNumIdStr = (string) element
                            .Elements(W.abstractNumId)
                            .First()
                            .Attribute(W.val);

                        int abstractNumId;

                        if (!int.TryParse(abstractNumIdStr, out abstractNumId))
                        {
                            throw new DocumentBuilderException("Invalid document - invalid value for abstractNumId");
                        }

                        XElement abstractElement = oldNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(p => (int) p.Attribute(W.abstractNumId) == abstractNumId)
                            .First();

                        XElement nsidElement = abstractElement
                            .Element(W.nsid);

                        string abstractNSID = null;

                        if (nsidElement != null)
                        {
                            abstractNSID = (string) nsidElement
                                .Attribute(W.val);
                        }

                        XElement newAbstractElement = newNumbering
                            .Descendants()
                            .Elements(W.abstractNum)
                            .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                            .Where(p =>
                            {
                                XElement thisNsidElement = p.Element(W.nsid);

                                if (thisNsidElement == null)
                                {
                                    return false;
                                }

                                return (string) thisNsidElement.Attribute(W.val) == abstractNSID;
                            })
                            .FirstOrDefault();

                        if (newAbstractElement == null)
                        {
                            newAbstractElement = new XElement(abstractElement);
                            newAbstractElement.Attribute(W.abstractNumId).Value = abstractNumber.ToString();
                            abstractNumber++;

                            if (newNumbering.Root.Elements(W.abstractNum).Any())
                            {
                                newNumbering.Root.Elements(W.abstractNum).Last().AddAfterSelf(newAbstractElement);
                            }
                            else
                            {
                                newNumbering.Root.Add(newAbstractElement);
                            }

                            foreach (XElement pictId in newAbstractElement.Descendants(W.lvlPicBulletId))
                            {
                                var bulletId = (string) pictId.Attribute(W.val);

                                XElement numPicBullet = oldNumbering
                                    .Descendants(W.numPicBullet)
                                    .FirstOrDefault(d => (string) d.Attribute(W.numPicBulletId) == bulletId);

                                int maxNumPicBulletId = new[] { -1 }.Concat(newNumbering.Descendants(W.numPicBullet)
                                            .Attributes(W.numPicBulletId)
                                            .Select(a => (int) a))
                                        .Max() +
                                    1;

                                var newNumPicBullet = new XElement(numPicBullet);
                                newNumPicBullet.Attribute(W.numPicBulletId).Value = maxNumPicBulletId.ToString();
                                pictId.Attribute(W.val).Value = maxNumPicBulletId.ToString();
                                newNumbering.Root.AddFirst(newNumPicBullet);
                            }
                        }

                        string newAbstractId = newAbstractElement.Attribute(W.abstractNumId).Value;

                        // Copy numbering element, if necessary (use matching element with no overrides)
                        XElement newElement;

                        if (numIdMap.ContainsKey(numId))
                        {
                            newElement = newNumbering
                                .Descendants()
                                .Elements(W.num)
                                .Where(e => e.Annotation<FromPreviousSourceSemaphore>() == null)
                                .Where(p => (int) p.Attribute(W.numId) == numIdMap[numId])
                                .First();
                        }
                        else
                        {
                            newElement = new XElement(element);

                            newElement
                                .Elements(W.abstractNumId)
                                .First()
                                .Attribute(W.val)
                                .Value = newAbstractId;

                            newElement.Attribute(W.numId).Value = number.ToString();
                            numIdMap.Add(numId, number);
                            number++;
                            newNumbering.Root.Add(newElement);
                        }

                        idElement.Attribute(W.val).Value = newElement.Attribute(W.numId).Value;
                    }
                }
            }

            if (newNumbering != null)
            {
                foreach (XElement abstractNum in newNumbering.Descendants(W.abstractNum))
                {
                    abstractNum.AddAnnotation(new FromPreviousSourceSemaphore());
                }

                foreach (XElement num in newNumbering.Descendants(W.num))
                {
                    num.AddAnnotation(new FromPreviousSourceSemaphore());
                }
            }

            if (newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart != null &&
                sourceNumberingPart != null)
            {
                AddRelationships(sourceNumberingPart,
                    newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart.GetXElement() });

                CopyRelatedPartsForContentParts(sourceNumberingPart,
                    newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart,
                    new[] { newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart.GetXElement() }, images);
            }

            if (newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart != null)
            {
                newDocument.MainDocumentPart.GlossaryDocumentPart.NumberingDefinitionsPart.SaveXDocument();
            }
        }

        private static void CopyRelatedImage(
            OpenXmlPart oldContentPart,
            OpenXmlPart newContentPart,
            XElement imageReference,
            XName attributeName,
            List<ImageData> images)
        {
            var relId = (string) imageReference.Attribute(attributeName);

            if (string.IsNullOrEmpty(relId))
            {
                return;
            }

            // First look to see if this relId has already been added to the new document.
            // This is necessary for those parts that get processed with both old and new ids, such as the comments
            // part.  This is not necessary for parts such as the main document part, but this code won't malfunction
            // in that case.
            IdPartPair tempPartIdPair5 = newContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

            if (tempPartIdPair5 != null)
            {
                return;
            }

            ExternalRelationship tempEr5 = newContentPart.ExternalRelationships.FirstOrDefault(er => er.Id == relId);

            if (tempEr5 != null)
            {
                return;
            }

            IdPartPair ipp2 = oldContentPart.Parts.FirstOrDefault(ipp => ipp.RelationshipId == relId);

            if (ipp2 != null)
            {
                OpenXmlPart oldPart2 = ipp2.OpenXmlPart;

                if (!(oldPart2 is ImagePart))
                {
                    throw new DocumentBuilderException("Invalid document - target part is not ImagePart");
                }

                var oldPart = (ImagePart) ipp2.OpenXmlPart;
                ImageData temp = ManageImageCopy(oldPart, newContentPart, images);

                if (temp.ImagePart == null)
                {
                    ImagePart newPart = null;

                    if (newContentPart is MainDocumentPart)
                    {
                        newPart = ((MainDocumentPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is HeaderPart)
                    {
                        newPart = ((HeaderPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is FooterPart)
                    {
                        newPart = ((FooterPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is EndnotesPart)
                    {
                        newPart = ((EndnotesPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is FootnotesPart)
                    {
                        newPart = ((FootnotesPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is ThemePart)
                    {
                        newPart = ((ThemePart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is WordprocessingCommentsPart)
                    {
                        newPart = ((WordprocessingCommentsPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is DocumentSettingsPart)
                    {
                        newPart = ((DocumentSettingsPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is ChartPart)
                    {
                        newPart = ((ChartPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is NumberingDefinitionsPart)
                    {
                        newPart = ((NumberingDefinitionsPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is DiagramDataPart)
                    {
                        newPart = ((DiagramDataPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    if (newContentPart is ChartDrawingPart)
                    {
                        newPart = ((ChartDrawingPart) newContentPart).AddImagePart(oldPart.ContentType);
                    }

                    temp.ImagePart = newPart;
                    string id = newContentPart.GetIdOfPart(newPart);
                    temp.AddContentPartRelTypeResourceIdTupple(newContentPart, newPart.RelationshipType, id);
                    imageReference.Attribute(attributeName).Value = id;
                    temp.WriteImage(newPart);
                }
                else
                {
                    IdPartPair refRel = newContentPart.Parts.FirstOrDefault(pip =>
                    {
                        ContentPartRelTypeIdTuple rel = temp.ContentPartRelTypeIdList.FirstOrDefault(cpr =>
                        {
                            bool found = cpr.ContentPart == newContentPart;
                            return found;
                        });

                        return rel != null;
                    });

                    if (refRel != null)
                    {
                        imageReference.Attribute(attributeName).Value = temp.ContentPartRelTypeIdList.First(cpr =>
                            {
                                bool found = cpr.ContentPart == newContentPart;
                                return found;
                            })
                            .RelationshipId;

                        return;
                    }

                    var g = new Guid();
                    string newId = $"R{g:N}".Substring(0, 16);
                    newContentPart.CreateRelationshipToPart(temp.ImagePart, newId);
                    imageReference.Attribute(R.id).Value = newId;
                }
            }
            else
            {
                ExternalRelationship er = oldContentPart.ExternalRelationships.FirstOrDefault(er1 => er1.Id == relId);

                if (er != null)
                {
                    ExternalRelationship newEr = newContentPart.AddExternalRelationship(er.RelationshipType, er.Uri);
                    imageReference.Attribute(R.id).Value = newEr.Id;
                }

                throw new DocumentBuilderInternalException(
                    "Source {0} is unsupported document - contains reference to NULL image");
            }
        }

        private static void CopyRelatedPartsForContentParts(
            OpenXmlPart oldContentPart,
            OpenXmlPart newContentPart,
            IReadOnlyCollection<XElement> newContent,
            List<ImageData> images)
        {
            List<XElement> relevantElements = newContent.DescendantsAndSelf()
                .Where(d => d.Name == VML.imagedata || d.Name == VML.fill || d.Name == VML.stroke || d.Name == A.blip)
                .ToList();

            foreach (XElement imageReference in relevantElements)
            {
                CopyRelatedImage(oldContentPart, newContentPart, imageReference, R.embed, images);
                CopyRelatedImage(oldContentPart, newContentPart, imageReference, R.pict, images);
                CopyRelatedImage(oldContentPart, newContentPart, imageReference, R.id, images);
            }

            foreach (XElement diagramReference in newContent.DescendantsAndSelf()
                         .Where(d => d.Name == DGM.relIds || d.Name == A.relIds))
            {
                // dm attribute
                string relId = diagramReference.Attribute(R.dm).Value;
                IdPartPair? ipp = newContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

                if (ipp != null)
                {
                    OpenXmlPart tempPart = ipp.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr = newContentPart.ExternalRelationships.FirstOrDefault(er2 => er2.Id == relId);

                if (tempEr != null)
                {
                    continue;
                }

                OpenXmlPart oldPart = oldContentPart.GetPartById(relId);
                OpenXmlPart newPart = newContentPart.AddNewPart<DiagramDataPart>();
                newPart.GetXDocument().Add(oldPart.GetXElement());
                diagramReference.Attribute(R.dm).Value = newContentPart.GetIdOfPart(newPart);
                AddRelationships(oldPart, newPart, new[] { newPart.GetXElement() });
                CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newPart.GetXElement() }, images);

                // lo attribute
                relId = diagramReference.Attribute(R.lo).Value;
                IdPartPair ipp2 = newContentPart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp2 != null)
                {
                    OpenXmlPart tempPart = ipp2.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr4 = newContentPart.ExternalRelationships.FirstOrDefault(er3 => er3.Id == relId);

                if (tempEr4 != null)
                {
                    continue;
                }

                oldPart = oldContentPart.GetPartById(relId);
                newPart = newContentPart.AddNewPart<DiagramLayoutDefinitionPart>();
                newPart.GetXDocument().Add(oldPart.GetXElement());
                diagramReference.Attribute(R.lo).Value = newContentPart.GetIdOfPart(newPart);
                AddRelationships(oldPart, newPart, new[] { newPart.GetXElement() });
                CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newPart.GetXElement() }, images);

                // qs attribute
                relId = diagramReference.Attribute(R.qs).Value;
                IdPartPair ipp5 = newContentPart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp5 != null)
                {
                    OpenXmlPart tempPart = ipp5.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr5 = newContentPart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr5 != null)
                {
                    continue;
                }

                oldPart = oldContentPart.GetPartById(relId);
                newPart = newContentPart.AddNewPart<DiagramStylePart>();
                newPart.GetXDocument().Add(oldPart.GetXElement());
                diagramReference.Attribute(R.qs).Value = newContentPart.GetIdOfPart(newPart);
                AddRelationships(oldPart, newPart, new[] { newPart.GetXElement() });
                CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newPart.GetXElement() }, images);

                // cs attribute
                relId = diagramReference.Attribute(R.cs).Value;
                IdPartPair ipp6 = newContentPart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp6 != null)
                {
                    OpenXmlPart tempPart = ipp6.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr6 = newContentPart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr6 != null)
                {
                    continue;
                }

                oldPart = oldContentPart.GetPartById(relId);
                newPart = newContentPart.AddNewPart<DiagramColorsPart>();
                newPart.GetXDocument().Add(oldPart.GetXElement());
                diagramReference.Attribute(R.cs).Value = newContentPart.GetIdOfPart(newPart);
                AddRelationships(oldPart, newPart, new[] { newPart.GetXElement() });
                CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newPart.GetXElement() }, images);
            }

            foreach (XElement oleReference in newContent.DescendantsAndSelf(O.OLEObject))
            {
                var relId = (string) oleReference.Attribute(R.id);

                // First look to see if this relId has already been added to the new document.
                // This is necessary for those parts that get processed with both old and new ids, such as the comments
                // part.  This is not necessary for parts such as the main document part, but this code won't malfunction
                // in that case.
                IdPartPair ipp1 = newContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

                if (ipp1 != null)
                {
                    OpenXmlPart tempPart = ipp1.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr1 = newContentPart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr1 != null)
                {
                    continue;
                }

                IdPartPair ipp4 = oldContentPart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp4 != null)
                {
                    OpenXmlPart oldPart = oldContentPart.GetPartById(relId);
                    OpenXmlPart newPart = null;

                    if (oldPart is EmbeddedObjectPart)
                    {
                        if (newContentPart is HeaderPart)
                        {
                            newPart = ((HeaderPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }

                        if (newContentPart is FooterPart)
                        {
                            newPart = ((FooterPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }

                        if (newContentPart is MainDocumentPart)
                        {
                            newPart = ((MainDocumentPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }

                        if (newContentPart is FootnotesPart)
                        {
                            newPart = ((FootnotesPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }

                        if (newContentPart is EndnotesPart)
                        {
                            newPart = ((EndnotesPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }

                        if (newContentPart is WordprocessingCommentsPart)
                        {
                            newPart = ((WordprocessingCommentsPart) newContentPart).AddEmbeddedObjectPart(oldPart.ContentType);
                        }
                    }
                    else if (oldPart is EmbeddedPackagePart)
                    {
                        if (newContentPart is HeaderPart)
                        {
                            newPart = ((HeaderPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is FooterPart)
                        {
                            newPart = ((FooterPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is MainDocumentPart)
                        {
                            newPart = ((MainDocumentPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is FootnotesPart)
                        {
                            newPart = ((FootnotesPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is EndnotesPart)
                        {
                            newPart = ((EndnotesPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is WordprocessingCommentsPart)
                        {
                            newPart = ((WordprocessingCommentsPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }

                        if (newContentPart is ChartPart)
                        {
                            newPart = ((ChartPart) newContentPart).AddEmbeddedPackagePart(oldPart.ContentType);
                        }
                    }

                    using (Stream oldObject = oldPart.GetStream(FileMode.Open, FileAccess.Read))
                    {
                        using (Stream newObject = newPart.GetStream(FileMode.Create, FileAccess.ReadWrite))
                        {
                            int byteCount;
                            var buffer = new byte[65536];

                            while ((byteCount = oldObject.Read(buffer, 0, 65536)) != 0)
                            {
                                newObject.Write(buffer, 0, byteCount);
                            }
                        }
                    }

                    oleReference.Attribute(R.id).Value = newContentPart.GetIdOfPart(newPart);
                }
                else
                {
                    if (relId != null)
                    {
                        ExternalRelationship er = oldContentPart.GetExternalRelationship(relId);
                        ExternalRelationship newEr = newContentPart.AddExternalRelationship(er.RelationshipType, er.Uri);
                        oleReference.Attribute(R.id).Value = newEr.Id;
                    }
                }
            }

            foreach (XElement chartReference in newContent.DescendantsAndSelf(C.chart))
            {
                var relId = (string) chartReference.Attribute(R.id);

                if (string.IsNullOrEmpty(relId))
                {
                    continue;
                }

                IdPartPair ipp2 = newContentPart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp2 != null)
                {
                    OpenXmlPart tempPart = ipp2.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr2 = newContentPart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr2 != null)
                {
                    continue;
                }

                IdPartPair ipp3 = oldContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

                if (ipp3 == null)
                {
                    continue;
                }

                var oldPart = (ChartPart) ipp3.OpenXmlPart;
                XDocument oldChart = oldPart.GetXDocument();
                var newPart = newContentPart.AddNewPart<ChartPart>();
                XDocument newChart = newPart.GetXDocument();
                newChart.Add(oldChart.Root);
                chartReference.Attribute(R.id).Value = newContentPart.GetIdOfPart(newPart);
                CopyChartObjects(oldPart, newPart);
                CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newChart.Root }, images);
            }

            foreach (XElement userShape in newContent.DescendantsAndSelf(C.userShapes))
            {
                var relId = (string) userShape.Attribute(R.id);

                if (string.IsNullOrEmpty(relId))
                {
                    continue;
                }

                IdPartPair ipp4 = newContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

                if (ipp4 != null)
                {
                    OpenXmlPart tempPart = ipp4.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr4 = newContentPart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr4 != null)
                {
                    continue;
                }

                IdPartPair ipp5 = oldContentPart.Parts.FirstOrDefault(p => p.RelationshipId == relId);

                if (ipp5 != null)
                {
                    var oldPart = (ChartDrawingPart) ipp5.OpenXmlPart;
                    XDocument oldXDoc = oldPart.GetXDocument();
                    var newPart = newContentPart.AddNewPart<ChartDrawingPart>();
                    XDocument newXDoc = newPart.GetXDocument();
                    newXDoc.Add(oldXDoc.Root);
                    userShape.Attribute(R.id).Value = newContentPart.GetIdOfPart(newPart);
                    AddRelationships(oldPart, newPart, newContent);
                    CopyRelatedPartsForContentParts(oldPart, newPart, new[] { newXDoc.Root }, images);
                }
            }
        }

        private static void CopyFontTable(FontTablePart oldFontTablePart, FontTablePart newFontTablePart)
        {
            List<XElement> relevantElements = oldFontTablePart.GetXDocument()
                .Descendants()
                .Where(d => d.Name == W.embedRegular ||
                    d.Name == W.embedBold ||
                    d.Name == W.embedItalic ||
                    d.Name == W.embedBoldItalic)
                .ToList();

            foreach (XElement fontReference in relevantElements)
            {
                var relId = (string) fontReference.Attribute(R.id);

                if (string.IsNullOrEmpty(relId))
                {
                    continue;
                }

                IdPartPair ipp1 = newFontTablePart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp1 != null)
                {
                    OpenXmlPart tempPart = ipp1.OpenXmlPart;
                    continue;
                }

                ExternalRelationship tempEr1 = newFontTablePart.ExternalRelationships.FirstOrDefault(z => z.Id == relId);

                if (tempEr1 != null)
                {
                    continue;
                }

                OpenXmlPart oldPart2 = oldFontTablePart.GetPartById(relId);

                if (oldPart2 == null || !(oldPart2 is FontPart))
                {
                    throw new DocumentBuilderException("Invalid document - FontTablePart contains invalid relationship");
                }

                var oldPart = (FontPart) oldPart2;
                FontPart newPart = newFontTablePart.AddFontPart(oldPart.ContentType);
                string ResourceID = newFontTablePart.GetIdOfPart(newPart);

                using (Stream oldFont = oldPart.GetStream(FileMode.Open, FileAccess.Read))
                {
                    using (Stream newFont = newPart.GetStream(FileMode.Create, FileAccess.ReadWrite))
                    {
                        int byteCount;
                        var buffer = new byte[65536];

                        while ((byteCount = oldFont.Read(buffer, 0, 65536)) != 0)
                        {
                            newFont.Write(buffer, 0, byteCount);
                        }
                    }
                }

                fontReference.Attribute(R.id).Value = ResourceID;
            }
        }

        private static void CopyChartObjects(ChartPart oldChart, ChartPart newChart)
        {
            foreach (XElement dataReference in newChart.GetXDocument().Descendants(C.externalData))
            {
                string relId = dataReference.Attribute(R.id).Value;

                IdPartPair ipp1 = oldChart.Parts.FirstOrDefault(z => z.RelationshipId == relId);

                if (ipp1 != null)
                {
                    OpenXmlPart oldRelatedPart = ipp1.OpenXmlPart;

                    if (oldRelatedPart is EmbeddedPackagePart)
                    {
                        var oldPart = (EmbeddedPackagePart) ipp1.OpenXmlPart;
                        EmbeddedPackagePart newPart = newChart.AddEmbeddedPackagePart(oldPart.ContentType);

                        using (Stream oldObject = oldPart.GetStream(FileMode.Open, FileAccess.Read))
                        {
                            using (Stream newObject = newPart.GetStream(FileMode.Create, FileAccess.ReadWrite))
                            {
                                int byteCount;
                                var buffer = new byte[65536];

                                while ((byteCount = oldObject.Read(buffer, 0, 65536)) != 0)
                                {
                                    newObject.Write(buffer, 0, byteCount);
                                }
                            }
                        }

                        dataReference.Attribute(R.id).Value = newChart.GetIdOfPart(newPart);
                    }
                    else if (oldRelatedPart is EmbeddedObjectPart)
                    {
                        var oldPart = (EmbeddedObjectPart) ipp1.OpenXmlPart;
                        string relType = oldRelatedPart.RelationshipType;
                        string conType = oldRelatedPart.ContentType;
                        var g = new Guid();
                        string id = $"R{g:N}".Substring(0, 8);
                        ExtendedPart newPart = newChart.AddExtendedPart(relType, conType, ".bin", id);

                        using (Stream oldObject = oldPart.GetStream(FileMode.Open, FileAccess.Read))
                        {
                            using (Stream newObject = newPart.GetStream(FileMode.Create, FileAccess.ReadWrite))
                            {
                                int byteCount;
                                var buffer = new byte[65536];

                                while ((byteCount = oldObject.Read(buffer, 0, 65536)) != 0)
                                {
                                    newObject.Write(buffer, 0, byteCount);
                                }
                            }
                        }

                        dataReference.Attribute(R.id).Value = newChart.GetIdOfPart(newPart);
                    }
                }
                else
                {
                    ExternalRelationship oldRelationship = oldChart.GetExternalRelationship(relId);
                    var g = Guid.NewGuid();
                    var newRid = $"R{g:N}";
                    ExternalRelationship oldRel = oldChart.ExternalRelationships.FirstOrDefault(h => h.Id == relId);

                    if (oldRel == null)
                    {
                        throw new DocumentBuilderInternalException("Internal Error 0007");
                    }

                    newChart.AddExternalRelationship(oldRel.RelationshipType, oldRel.Uri, newRid);
                    dataReference.Attribute(R.id).Value = newRid;
                }
            }
        }

        private static void CopyStartingParts(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            List<ImageData> images)
        {
            // A Core File Properties part does not have implicit or explicit relationships to other parts.
            CoreFilePropertiesPart corePart = sourceDocument.CoreFilePropertiesPart;

            if (corePart?.GetXElement() != null)
            {
                newDocument.AddCoreFilePropertiesPart();
                XDocument newXDoc = newDocument.CoreFilePropertiesPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                XDocument sourceXDoc = corePart.GetXDocument();
                newXDoc.Add(sourceXDoc.Root);
            }

            // An application attributes part does not have implicit or explicit relationships to other parts.
            ExtendedFilePropertiesPart extPart = sourceDocument.ExtendedFilePropertiesPart;

            if (extPart != null)
            {
                OpenXmlPart newPart = newDocument.AddExtendedFilePropertiesPart();
                XDocument newXDoc = newDocument.ExtendedFilePropertiesPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                newXDoc.Add(extPart.GetXElement());
            }

            // An custom file properties part does not have implicit or explicit relationships to other parts.
            CustomFilePropertiesPart customPart = sourceDocument.CustomFilePropertiesPart;

            if (customPart != null)
            {
                newDocument.AddCustomFilePropertiesPart();
                XDocument newXDoc = newDocument.CustomFilePropertiesPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                newXDoc.Add(customPart.GetXElement());
            }

            DocumentSettingsPart oldSettingsPart = sourceDocument.MainDocumentPart.DocumentSettingsPart;

            if (oldSettingsPart != null)
            {
                var newSettingsPart = newDocument.MainDocumentPart.AddNewPart<DocumentSettingsPart>();
                XDocument settingsXDoc = oldSettingsPart.GetXDocument();
                AddRelationships(oldSettingsPart, newSettingsPart, new[] { settingsXDoc.Root });
                CopyFootnotesPart(sourceDocument, newDocument, settingsXDoc, images);
                CopyEndnotesPart(sourceDocument, newDocument, settingsXDoc, images);
                XDocument newXDoc = newDocument.MainDocumentPart.DocumentSettingsPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                newXDoc.Add(settingsXDoc.Root);
                CopyRelatedPartsForContentParts(oldSettingsPart, newSettingsPart, new[] { newXDoc.Root }, images);
            }

            WebSettingsPart oldWebSettingsPart = sourceDocument.MainDocumentPart.WebSettingsPart;

            if (oldWebSettingsPart != null)
            {
                var newWebSettingsPart = newDocument.MainDocumentPart.AddNewPart<WebSettingsPart>();
                XDocument settingsXDoc = oldWebSettingsPart.GetXDocument();
                AddRelationships(oldWebSettingsPart, newWebSettingsPart, new[] { settingsXDoc.Root });
                XDocument newXDoc = newDocument.MainDocumentPart.WebSettingsPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                newXDoc.Add(settingsXDoc.Root);
            }

            ThemePart themePart = sourceDocument.MainDocumentPart.ThemePart;

            if (themePart != null)
            {
                var newThemePart = newDocument.MainDocumentPart.AddNewPart<ThemePart>();
                XDocument newXDoc = newDocument.MainDocumentPart.ThemePart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                newXDoc.Add(themePart.GetXElement());
                CopyRelatedPartsForContentParts(themePart, newThemePart, new[] { newThemePart.GetXElement() }, images);
            }

            // If needed to handle GlossaryDocumentPart in the future, then
            // this code should handle the following parts:
            //   MainDocumentPart.GlossaryDocumentPart.StyleDefinitionsPart
            //   MainDocumentPart.GlossaryDocumentPart.StylesWithEffectsPart

            // A Style Definitions part shall not have implicit or explicit relationships to any other part.
            StyleDefinitionsPart stylesPart = sourceDocument.MainDocumentPart.StyleDefinitionsPart;

            if (stylesPart != null)
            {
                newDocument.MainDocumentPart.AddNewPart<StyleDefinitionsPart>();
                XDocument newXDoc = newDocument.MainDocumentPart.StyleDefinitionsPart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;

                newXDoc.Add(new XElement(W.styles,
                    new XAttribute(XNamespace.Xmlns + "w", W.w)

                    //,
                    //stylesPart.GetXDocument().Descendants(W.docDefaults)

                    //,
                    //new XElement(W.latentStyles, stylesPart.GetXDocument().Descendants(W.latentStyles).Attributes())
                ));

                MergeDocDefaultStyles(stylesPart.GetXDocument(), newXDoc);
                MergeStyles(sourceDocument, newDocument, stylesPart.GetXDocument(), newXDoc, Enumerable.Empty<XElement>());
                MergeLatentStyles(stylesPart.GetXDocument(), newXDoc);
            }

            // A Font Table part shall not have any implicit or explicit relationships to any other part.
            FontTablePart fontTablePart = sourceDocument.MainDocumentPart.FontTablePart;

            if (fontTablePart != null)
            {
                newDocument.MainDocumentPart.AddNewPart<FontTablePart>();
                XDocument newXDoc = newDocument.MainDocumentPart.FontTablePart.GetXDocument();
                newXDoc.Declaration.Standalone = Yes;
                newXDoc.Declaration.Encoding = Utf8;
                CopyFontTable(sourceDocument.MainDocumentPart.FontTablePart, newDocument.MainDocumentPart.FontTablePart);
                newXDoc.Add(fontTablePart.GetXElement());
            }
        }

        private static void CopyFootnotesPart(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            XDocument settingsXDoc,
            List<ImageData> images)
        {
            var number = 0;
            XDocument oldFootnotes = null;
            XDocument newFootnotes = null;
            XElement footnotePr = settingsXDoc.Root.Element(W.footnotePr);

            if (footnotePr == null)
            {
                return;
            }

            if (sourceDocument.MainDocumentPart.FootnotesPart == null)
            {
                return;
            }

            foreach (XElement footnote in footnotePr.Elements(W.footnote))
            {
                if (oldFootnotes == null)
                {
                    oldFootnotes = sourceDocument.MainDocumentPart.FootnotesPart.GetXDocument();
                }

                if (newFootnotes == null)
                {
                    if (newDocument.MainDocumentPart.FootnotesPart != null)
                    {
                        newFootnotes = newDocument.MainDocumentPart.FootnotesPart.GetXDocument();
                        newFootnotes.Declaration.Standalone = Yes;
                        newFootnotes.Declaration.Encoding = Utf8;
                        IEnumerable<int> ids = newFootnotes.Root.Elements(W.footnote).Select(f => (int) f.Attribute(W.id));

                        if (ids.Any())
                        {
                            number = ids.Max() + 1;
                        }
                    }
                    else
                    {
                        newDocument.MainDocumentPart.AddNewPart<FootnotesPart>();
                        newFootnotes = newDocument.MainDocumentPart.FootnotesPart.GetXDocument();
                        newFootnotes.Declaration.Standalone = Yes;
                        newFootnotes.Declaration.Encoding = Utf8;
                        newFootnotes.Add(new XElement(W.footnotes, NamespaceAttributes));
                    }
                }

                var id = (string) footnote.Attribute(W.id);

                XElement element = oldFootnotes.Descendants()
                    .Elements(W.footnote)
                    .Where(p => (string) p.Attribute(W.id) == id)
                    .FirstOrDefault();

                if (element != null)
                {
                    var newElement = new XElement(element);
                    // the following adds the footnote into the new settings part
                    newElement.Attribute(W.id).Value = number.ToString();
                    newFootnotes.Root.Add(newElement);
                    footnote.Attribute(W.id).Value = number.ToString();
                    number++;
                }
            }
        }

        private static void CopyEndnotesPart(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            XDocument settingsXDoc,
            List<ImageData> images)
        {
            var number = 0;
            XDocument oldEndnotes = null;
            XDocument newEndnotes = null;
            XElement endnotePr = settingsXDoc.Root.Element(W.endnotePr);

            if (endnotePr == null)
            {
                return;
            }

            if (sourceDocument.MainDocumentPart.EndnotesPart == null)
            {
                return;
            }

            foreach (XElement endnote in endnotePr.Elements(W.endnote))
            {
                if (oldEndnotes == null)
                {
                    oldEndnotes = sourceDocument.MainDocumentPart.EndnotesPart.GetXDocument();
                }

                if (newEndnotes == null)
                {
                    if (newDocument.MainDocumentPart.EndnotesPart != null)
                    {
                        newEndnotes = newDocument.MainDocumentPart.EndnotesPart.GetXDocument();
                        newEndnotes.Declaration.Standalone = Yes;
                        newEndnotes.Declaration.Encoding = Utf8;

                        IEnumerable<int> ids = newEndnotes.Root
                            .Elements(W.endnote)
                            .Select(f => (int) f.Attribute(W.id));

                        if (ids.Any())
                        {
                            number = ids.Max() + 1;
                        }
                    }
                    else
                    {
                        newDocument.MainDocumentPart.AddNewPart<EndnotesPart>();
                        newEndnotes = newDocument.MainDocumentPart.EndnotesPart.GetXDocument();
                        newEndnotes.Declaration.Standalone = Yes;
                        newEndnotes.Declaration.Encoding = Utf8;
                        newEndnotes.Add(new XElement(W.endnotes, NamespaceAttributes));
                    }
                }

                var id = (string) endnote.Attribute(W.id);

                XElement element = oldEndnotes.Descendants()
                    .Elements(W.endnote)
                    .Where(p => (string) p.Attribute(W.id) == id)
                    .FirstOrDefault();

                if (element != null)
                {
                    var newElement = new XElement(element);
                    newElement.Attribute(W.id).Value = number.ToString();
                    newEndnotes.Root.Add(newElement);
                    endnote.Attribute(W.id).Value = number.ToString();
                    number++;
                }
            }
        }

        private static void FixRanges(XElement sourceRootElement, IReadOnlyCollection<XElement> newContent)
        {
            FixRange(sourceRootElement,
                newContent,
                W.commentRangeStart,
                W.commentRangeEnd,
                W.id,
                W.commentReference);

            FixRange(sourceRootElement,
                newContent,
                W.bookmarkStart,
                W.bookmarkEnd,
                W.id,
                null);

            FixRange(sourceRootElement,
                newContent,
                W.permStart,
                W.permEnd,
                W.id,
                null);

            FixRange(sourceRootElement,
                newContent,
                W.moveFromRangeStart,
                W.moveFromRangeEnd,
                W.id,
                null);

            FixRange(sourceRootElement,
                newContent,
                W.moveToRangeStart,
                W.moveToRangeEnd,
                W.id,
                null);

            DeleteUnmatchedRange(newContent,
                W.moveFromRangeStart,
                W.moveFromRangeEnd,
                W.moveToRangeStart,
                W.name,
                W.id);

            DeleteUnmatchedRange(newContent,
                W.moveToRangeStart,
                W.moveToRangeEnd,
                W.moveFromRangeStart,
                W.name,
                W.id);
        }

        private static void AddAtBeginning(IEnumerable<XElement> newContent, XElement contentToAdd)
        {
            if (newContent.First().Element(W.pPr) != null)
            {
                newContent.First().Element(W.pPr).AddAfterSelf(contentToAdd);
            }
            else
            {
                newContent.First().AddFirst(new XElement(contentToAdd));
            }
        }

        private static void AddAtEnd(IEnumerable<XElement> newContent, XElement contentToAdd)
        {
            if (newContent.Last().Element(W.pPr) != null)
            {
                newContent.Last().Element(W.pPr).AddAfterSelf(new XElement(contentToAdd));
            }
            else
            {
                newContent.Last().Add(new XElement(contentToAdd));
            }
        }

        // If the set of paragraphs from sourceDocument don't have a complete start/end for bookmarks,
        // comments, etc., then this adds them to the paragraph.  Note that this adds them to
        // sourceDocument, and is impure.
        private static void FixRange(
            XElement sourceRootElement,
            IReadOnlyCollection<XElement> newContent,
            XName startElement,
            XName endElement,
            XName idAttribute,
            XName? refElement)
        {
            foreach (XElement start in newContent.DescendantsAndSelf(startElement))
            {
                string rangeId = start.Attribute(idAttribute)!.Value;

                if (newContent.DescendantsAndSelf(endElement).All(e => e.Attribute(idAttribute)!.Value != rangeId))
                {
                    XElement? end = sourceRootElement
                        .Descendants(endElement)
                        .FirstOrDefault(o => o.Attribute(idAttribute)!.Value == rangeId);

                    if (end is not null)
                    {
                        AddAtEnd(newContent, new XElement(end));

                        if (refElement is not null)
                        {
                            var newRef = new XElement(refElement, new XAttribute(idAttribute, rangeId));
                            AddAtEnd(newContent, new XElement(newRef));
                        }
                    }
                }
            }

            foreach (XElement end in newContent.Elements(endElement))
            {
                string rangeId = end.Attribute(idAttribute).Value;

                if (newContent
                        .DescendantsAndSelf(startElement)
                        .Where(s => s.Attribute(idAttribute).Value == rangeId)
                        .Count() ==
                    0)
                {
                    XElement start = sourceRootElement
                        .Descendants(startElement)
                        .Where(o => o.Attribute(idAttribute).Value == rangeId)
                        .FirstOrDefault();

                    if (start != null)
                    {
                        AddAtBeginning(newContent, new XElement(start));
                    }
                }
            }
        }

        private static void DeleteUnmatchedRange(
            IEnumerable<XElement> newContent,
            XName startElement,
            XName endElement,
            XName matchTo,
            XName matchAttr,
            XName idAttr)
        {
            var deleteList = new List<string>();

            foreach (XElement start in newContent.Elements(startElement))
            {
                string id = start.Attribute(matchAttr).Value;

                if (!newContent.Elements(matchTo).Where(n => n.Attribute(matchAttr).Value == id).Any())
                {
                    deleteList.Add(start.Attribute(idAttr).Value);
                }
            }

            foreach (string item in deleteList)
            {
                newContent.Elements(startElement).Where(n => n.Attribute(idAttr).Value == item).Remove();
                newContent.Elements(endElement).Where(n => n.Attribute(idAttr).Value == item).Remove();
                newContent.Where(p => p.Name == startElement && p.Attribute(idAttr).Value == item).Remove();
                newContent.Where(p => p.Name == endElement && p.Attribute(idAttr).Value == item).Remove();
            }
        }

        private static void CopyFootnotes(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent,
            List<ImageData> images)
        {
            var number = 0;
            XDocument oldFootnotes = null;
            XDocument newFootnotes = null;

            foreach (XElement footnote in newContent.DescendantsAndSelf(W.footnoteReference))
            {
                if (oldFootnotes == null)
                {
                    oldFootnotes = sourceDocument.MainDocumentPart.FootnotesPart.GetXDocument();
                }

                if (newFootnotes == null)
                {
                    if (newDocument.MainDocumentPart.FootnotesPart != null)
                    {
                        newFootnotes = newDocument.MainDocumentPart.FootnotesPart.GetXDocument();

                        IEnumerable<int> ids = newFootnotes
                            .Root
                            .Elements(W.footnote)
                            .Select(f => (int) f.Attribute(W.id));

                        if (ids.Any())
                        {
                            number = ids.Max() + 1;
                        }
                    }
                    else
                    {
                        newDocument.MainDocumentPart.AddNewPart<FootnotesPart>();
                        newFootnotes = newDocument.MainDocumentPart.FootnotesPart.GetXDocument();
                        newFootnotes.Declaration.Standalone = Yes;
                        newFootnotes.Declaration.Encoding = Utf8;
                        newFootnotes.Add(new XElement(W.footnotes, NamespaceAttributes));
                    }
                }

                var id = (string) footnote.Attribute(W.id);

                XElement element = oldFootnotes
                    .Descendants()
                    .Elements(W.footnote)
                    .Where(p => (string) p.Attribute(W.id) == id)
                    .FirstOrDefault();

                if (element != null)
                {
                    var newElement = new XElement(element);
                    newElement.Attribute(W.id).Value = number.ToString();
                    newFootnotes.Root.Add(newElement);
                    footnote.Attribute(W.id).Value = number.ToString();
                    number++;
                }
            }

            if (sourceDocument.MainDocumentPart.FootnotesPart != null &&
                newDocument.MainDocumentPart.FootnotesPart != null)
            {
                AddRelationships(sourceDocument.MainDocumentPart.FootnotesPart,
                    newDocument.MainDocumentPart.FootnotesPart,
                    new[] { newDocument.MainDocumentPart.FootnotesPart.GetXElement() });

                CopyRelatedPartsForContentParts(sourceDocument.MainDocumentPart.FootnotesPart,
                    newDocument.MainDocumentPart.FootnotesPart,
                    new[] { newDocument.MainDocumentPart.FootnotesPart.GetXElement() }, images);
            }
        }

        private static void CopyEndnotes(
            WordprocessingDocument sourceDocument,
            WordprocessingDocument newDocument,
            IEnumerable<XElement> newContent,
            List<ImageData> images)
        {
            var number = 0;
            XDocument oldEndnotes = null;
            XDocument newEndnotes = null;

            foreach (XElement endnote in newContent.DescendantsAndSelf(W.endnoteReference))
            {
                if (oldEndnotes == null)
                {
                    oldEndnotes = sourceDocument.MainDocumentPart.EndnotesPart.GetXDocument();
                }

                if (newEndnotes == null)
                {
                    if (newDocument.MainDocumentPart.EndnotesPart != null)
                    {
                        newEndnotes = newDocument
                            .MainDocumentPart
                            .EndnotesPart
                            .GetXDocument();

                        IEnumerable<int> ids = newEndnotes
                            .Root
                            .Elements(W.endnote)
                            .Select(f => (int) f.Attribute(W.id));

                        if (ids.Any())
                        {
                            number = ids.Max() + 1;
                        }
                    }
                    else
                    {
                        newDocument.MainDocumentPart.AddNewPart<EndnotesPart>();
                        newEndnotes = newDocument.MainDocumentPart.EndnotesPart.GetXDocument();
                        newEndnotes.Declaration.Standalone = Yes;
                        newEndnotes.Declaration.Encoding = Utf8;
                        newEndnotes.Add(new XElement(W.endnotes, NamespaceAttributes));
                    }
                }

                var id = (string) endnote.Attribute(W.id);

                XElement element = oldEndnotes
                    .Descendants()
                    .Elements(W.endnote)
                    .Where(p => (string) p.Attribute(W.id) == id)
                    .First();

                var newElement = new XElement(element);
                newElement.Attribute(W.id).Value = number.ToString();
                newEndnotes.Root.Add(newElement);
                endnote.Attribute(W.id).Value = number.ToString();
                number++;
            }

            if (sourceDocument.MainDocumentPart.EndnotesPart != null &&
                newDocument.MainDocumentPart.EndnotesPart != null)
            {
                AddRelationships(sourceDocument.MainDocumentPart.EndnotesPart,
                    newDocument.MainDocumentPart.EndnotesPart,
                    new[] { newDocument.MainDocumentPart.EndnotesPart.GetXElement() });

                CopyRelatedPartsForContentParts(sourceDocument.MainDocumentPart.EndnotesPart,
                    newDocument.MainDocumentPart.EndnotesPart,
                    new[] { newDocument.MainDocumentPart.EndnotesPart.GetXElement() }, images);
            }
        }

        // General function for handling images that tries to use an existing image if they are the same
        private static ImageData ManageImageCopy(ImagePart oldImage, OpenXmlPart newContentPart, List<ImageData> images)
        {
            var oldImageData = new ImageData(oldImage);

            foreach (ImageData item in images)
            {
                if (newContentPart != item.ImagePart)
                {
                    continue;
                }

                if (item.Compare(oldImageData))
                {
                    return item;
                }
            }

            images.Add(oldImageData);
            return oldImageData;
        }
    }
}
