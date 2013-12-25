using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public class TreeView : ITreeView
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
        Dictionary<string, ZNodesDataStructures.MachineNode> machines = new Dictionary<string, ZNodesDataStructures.MachineNode>();

        public delegate void machineDataChangeDelegate(ITreeView tree, string machineId, ZNodesDataStructures.MachineNode oldData);
        public  machineDataChangeDelegate machineDataChange;

        // Sets the handler for machine data change (any machine)
        public machineDataChangeDelegate MachineDataChange { set { machineDataChange = value; } }

        public Dictionary<string, Uri> Snapshot
        {
            get { return snapshot; }
        }

        public Dictionary<string, ZNodesDataStructures.MachineNode> Machines
        {
            get { return machines; }
        }

        public void setMachineDataChangeCallback(machineDataChangeDelegate mdcd)
        {
            machineDataChange = mdcd;
        }

        public void setMachineData(string machineId, ZNodesDataStructures.MachineNode data)
        {
            ZNodesDataStructures.MachineNode oldData = null;
            if (machines.ContainsKey(machineId))
            {
                oldData = machines[machineId];
            }
            machines.Add(machineId, data);
            machineDataChange((ITreeView)this, machineId, oldData);
        }
    }
}
