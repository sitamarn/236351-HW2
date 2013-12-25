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
            public List<String> primaryOf = new List<String>();
            public List<String> backsUp = new List<String>();
        }

        [Serializable]
        public class SellerNode
        {
            public int version;
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
