using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class TreeView : ITreeView
    {
        private static readonly TreeView instance = new TreeView();

        public static TreeView Instance
        {
            get
            {
                return instance;
            }
        }

        private Dictionary<string, Uri> snapshot = new Dictionary<string, Uri>();

        public Dictionary<string, Uri> Snapshot { get { return snapshot; } }
    }
}
