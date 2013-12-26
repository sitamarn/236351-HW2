using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public static class ZNodesDataStructures
    {
        [Serializable]
        public class MachineNode
        {
            public String originalSellerName = "";
            public Uri uri = null;
            public List<String> primaryOf = new List<String>();
            public List<String> backsUp = new List<String>();

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Original seller name: " + originalSellerName);
                sb.AppendLine("Machine URI: " + uri.ToString());
                sb.AppendLine("Primary of: " + String.Join(" ", primaryOf));
                sb.AppendLine("Backup of: " + String.Join(" ", backsUp));
                return sb.ToString();
            }
        }

        [Serializable]
        public class SellerNode
        {
            public enum NodeRole { Backup, Main }

            public int version;
            public Uri uri = null;
            public String nodeId = null;
            public NodeRole role = NodeRole.Main;
        }

        public static byte[] serialize<T>(T sz)
        {
            if (sz == null) { return null; }
            byte[] content = null;
            try
            {
                IFormatter bf = new BinaryFormatter();
                using (MemoryStream ms = new MemoryStream())
                {
                    using (var ds = new DeflateStream(ms, CompressionMode.Compress, true))
                    {
                        bf.Serialize(ds, sz);
                    }
                    ms.Position = 0;
                    content = ms.GetBuffer();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Serialization process failed - " + ex.Message);
            }
            return content;
        }

        public static T deserialize<T>(byte[] sz) where T : new()
        {
            if (sz == null) { return default(T); }
            var nodeData = new T();
            try
            {
                var formatter = new BinaryFormatter();
                using (var ms = new MemoryStream(sz))
                {
                    using (var ds = new DeflateStream(ms, CompressionMode.Decompress, true))
                    {
                        nodeData = (T)formatter.Deserialize(ds);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deserializing machine - " + ex.Message);
            }

            return nodeData;
        }
    }
}
