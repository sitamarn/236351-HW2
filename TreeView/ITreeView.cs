using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public interface ITreeView
    {
        Dictionary<string, Uri> Snapshot { get; }

        Dictionary<string, ZNodesDataStructures.MachineNode> Machines { get; }

        String Id { get; }

        //public static ITreeView Instance { abstract get; }
    }
}
