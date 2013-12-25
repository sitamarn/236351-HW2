using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public delegate void VersionRefresh(String sellerName, List<Uri> machines);
    public delegate void MachineJoined(String sellerName, Uri machine);
    public delegate void MachineDropped(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup);

    public interface IReplicationModuleEvents
    {
        VersionRefresh VersionRefreshHandler { set; }
        MachineJoined MachineJoinedHandler { set; }
        MachineDropped MachineDroppedHandler { set; }
    }
}
