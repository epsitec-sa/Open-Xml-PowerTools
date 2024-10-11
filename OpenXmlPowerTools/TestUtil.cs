// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace OpenXmlPowerTools
{
    public class TestUtil
    {
        private static bool? s_DeleteTempFiles = null;

        public static bool DeleteTempFiles
        {
            get
            {
                if (s_DeleteTempFiles != null)
                    return (bool)s_DeleteTempFiles;
                FileInfo donotdelete = new FileInfo("donotdelete.txt");
                s_DeleteTempFiles = !donotdelete.Exists;
                return (bool)s_DeleteTempFiles;
            }
        }

        private static DirectoryInfo s_SourceDir = null;
        private static DirectoryInfo s_TempDir = null;
        public static DirectoryInfo TempDir
        {
            get
            {
                if (s_TempDir != null)
                    return s_TempDir;
                else
                {
                    var now = DateTime.Now;
                    var tempDirName = String.Format("Test-{0:00}-{1:00}-{2:00}-{3:00}{4:00}{5:00}", now.Year - 2000, now.Month, now.Day, now.Hour, now.Minute, now.Second);
                    s_TempDir = new DirectoryInfo(Path.Combine(".", tempDirName));
                    s_TempDir.Create();
                    return s_TempDir;
                }
            }
        }
        public static DirectoryInfo SourceDir
        {
            get
            {
                if (s_SourceDir != null)
                {
                    return s_SourceDir;
                }
                else
                {
                    var root = GetDirectoryNameOfFolderAbove (Assembly.GetExecutingAssembly().Location, "open-xml-powertools");
                    s_SourceDir = new DirectoryInfo (Path.Combine (root, "TestFiles"));
                    return s_SourceDir;
                }
            }
        }

        public static void NotePad(string str)
        {
            var guidName = Guid.NewGuid().ToString().Replace("-", "") + ".txt";
            var fi = new FileInfo(Path.Combine(TempDir.FullName, guidName));
            File.WriteAllText(fi.FullName, str);
            var notepadExe = new FileInfo(@"C:\Program Files (x86)\Notepad++\notepad++.exe");
            if (!notepadExe.Exists)
                notepadExe = new FileInfo(@"C:\Program Files\Notepad++\notepad++.exe");
            if (!notepadExe.Exists)
                notepadExe = new FileInfo(@"C:\Windows\System32\notepad.exe");
            ExecutableRunner.RunExecutable(notepadExe.FullName, fi.FullName, TempDir.FullName);
        }

        public static void KDiff3(FileInfo oldFi, FileInfo newFi)
        {
            var kdiffExe = new FileInfo(@"C:\Program Files (x86)\KDiff3\kdiff3.exe");
            var result = ExecutableRunner.RunExecutable(kdiffExe.FullName, oldFi.FullName + " " + newFi.FullName, TempDir.FullName);
        }

        public static void Explorer(DirectoryInfo di)
        {
            Process.Start(di.FullName);
        }

        public static string GetDirectoryNameOfFolderAbove(string startFromDir, string folderName)
        {
            var currentDir = Directory.GetParent (startFromDir);
            while (currentDir != null)
            {
                var name = Directory
                    .EnumerateDirectories (currentDir.FullName, folderName)
                    .SingleOrDefault ();

                if (name != null)
                {
                    return name;
                }

                currentDir = Directory.GetParent (currentDir.FullName);
            }
            return null;
        }

    }
}
