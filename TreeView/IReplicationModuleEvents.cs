using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public delegate void VersionRefresh(String sellerName, List<Uri> machines);
    public delegate void MachineJoined(String machineName, String sellerName, Uri machine);
    public delegate void MachineDropped(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup);
    public delegate void LoadBalancingDone();

    public interface IReplicationModuleEvents
    {
        MachineJoined MachineJoinedHandler { set; }
        MachineDropped MachineDroppedHandler { set; }
        LoadBalancingDone LoadBalancingDoneHandler { set; }
    }
}
