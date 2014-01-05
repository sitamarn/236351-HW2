using Registeration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;
using TreeViewLib;

namespace AirlineServer
{
    class IntraClusterService: ISellerClusterService
    {
        CacheData cache;
        List<AirlineServer.Seller> primaries;
        List<AirlineServer.Seller> backups;
        bool isLeader = false;
        string myName = null;
        string mysearchServerAddress;
        string myAddress;
        string clusterName;

        public IntraClusterService(CacheData cacheData,Seller initialSeller, string searchServerAddress, string thisAddress, string clusterName)
        {
            cache = cacheData;
            primaries = new List<Seller>();
            primaries.Add(initialSeller);
            backups = new List<Seller>();      
            mysearchServerAddress= searchServerAddress;
            myAddress = thisAddress;
            this.clusterName = clusterName;
        }

        public void setName(string name)
        {
            myName = name;
        }


        private List<string> getTheMostBusyMachineOrderByPrimaries(Dictionary<string, ZNodesDataStructures.MachineNode> machines){

            return (from p in machines orderby p.Value.primaryOf.Count descending, p.Key ascending select p.Key ).ToList();
        }

        private List<string> getTheMostBusyMachineOrderByBackups(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            return (from p in machines orderby p.Value.backsUp.Count descending, p.Key ascending select p.Key).ToList();
        }

        private bool checkIfLeader()
        {
            var machines = AirlineReplicationModule.Instance.Machines;
            var keys = machines.Keys;
            return myName.Equals(keys.Min());
            
        }

        public void respondIfNewNode(String machineName, String sellerName, Uri machine)
        {
            // Clear the cache because the system changed -> the data changed
            cache.clear();

            //Barrier - Create, Enter, BLOCK -> Leave when Balancing ends
            var barrier = AirlineReplicationModule.Instance.barrier();

            // Get the cuurent snapshot of the system - including the new machine that came up!
            //not included the sellers that it holds.
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = AirlineReplicationModule.Instance.Machines;

            // in the ZK tree: If a machine holds a seller that has return to live - drop it!
            foreach (string smachine in machines.Keys)
            {
                if (!smachine.Equals(machineName))
                {
                    machines[smachine].primaryOf.Remove(sellerName);
                    machines[smachine].backsUp.Remove(sellerName);
                }
            }

            // this is the oldest legal view of the tree. necessary to enable sellers transfers
            Dictionary<string, ZNodesDataStructures.MachineNode> machinesOldVersion = new Dictionary<string, ZNodesDataStructures.MachineNode>(machines);

            // in the local data: If a this machine holds an old version of the seller - drop it!!
            if (!machineName.Equals(myName))
            {
                primaries.RemoveAll(delegate(Seller candidate)
                {
                    return candidate.name.Equals(sellerName);
                });

                backups.RemoveAll(delegate(Seller candidate)
                {
                    return candidate.name.Equals(sellerName);
                });
            }

            // the new machine holds the new seller
            // If there is only a single machine there is no need to backup the seller and load balancing
            if (machines.Count == 1)
            {
                try
                {
                    ServiceEndpoint sellerSearchEndPoint =
                    new ServiceEndpoint(ContractDescription.GetContract(
                        typeof(IAirSellerRegisteration)),
                        new WebHttpBinding(),
                        new EndpointAddress
                        (mysearchServerAddress));
                    using (WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(sellerSearchEndPoint))
                    {
                        IAirSellerRegisteration registerChannel = cf.CreateChannel();
                        //Register the channel in the server
                        registerChannel.RegisterSeller(new Uri(myAddress), clusterName);
                    }
                }
                catch (ProtocolException e)
                {
                    Console.WriteLine("Bad Protocol: " + e.Message);
                }
                catch (Exception e)
                {

                    if (e.InnerException is WebException)
                    {
                        HttpWebResponse resp = (HttpWebResponse)((WebException)e.InnerException).Response;
                        Console.WriteLine("Failed, {0}", resp.StatusDescription);
                    }
                    else
                    {
                        Console.WriteLine("Advertisement connection kicked the bucket, quitting because:");
                        Console.WriteLine(e.Message.ToString());
                    }
                    return;
                }
            }
            else
            {
                // if there are 2 machines - it means that before the current machine has joined there were no backups!
                if (machines.Count == 2)
                {
                    // get the oldest machine
                    string oldMachine = machines.Keys.Where(delegate(string str)
                    {
                        return !str.Equals(machineName);
                    }).First();

                    // backup all the sellers in the old machine
                    foreach (string sell in machines[oldMachine].primaryOf)
                    {
                        machines[machineName].backsUp.Add(sell);
                        if (machineName.Equals(myName))
                        {
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[oldMachine].uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                try
                                {
                                    Seller sellerToBackup = sellerCluster.sendPrimarySeller(sell);
                                    if (sellerToBackup == null)
                                    {
                                        sellerToBackup = sellerCluster.sendBackupSeller(sellerName);
                                    }
                                    backups.Add(sellerToBackup);
                                }
                                catch (FaultException fe)
                                {
                                    Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                    return;
                                }

                            }
                        }

                    }
                }

                // find a deterministic victim to hold the backup of the new seller
                string victim = (from p in machines where !p.Key.Equals(machineName) orderby p.Value.primaryOf.Count ascending, p.Key ascending select p.Key).First();

                // if the victim is this machine - follow the command and ask a clone from the new machine that owns the primary replica
                if (victim.Equals(myName))
                {

                    Uri uri = machine;

                    ServiceEndpoint endPoint = new ServiceEndpoint(
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[machineName].uri));
                    using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                    {
                        ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                        try
                        {
                            Seller sellerToBackup = sellerCluster.sendPrimarySeller(sellerName);
                            if (sellerToBackup == null)
                            {
                                sellerToBackup = sellerCluster.sendBackupSeller(sellerName);
                            }
                            backups.Add(sellerToBackup);
                        }
                        catch (FaultException fe)
                        {
                            Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                            return;
                        }

                    }

                }
                // update it in the ZK tree
                machines[victim].backsUp.Add(sellerName);


            }

            // execute a deterministic load-balancing algorithm
            if (machines.Keys.Count > 1)
            {
                Console.WriteLine("\t** BALANCING ALGORITHM STARTED **");
                balanceTheTreeAfterJoined(machines, machinesOldVersion, machineName, machine);
                // this lock makes sure that no search server will serviced while sellers are removed from the machine.
                Console.WriteLine("\t** BALANCING ALGORITHM FINISHED **");
            }




            //// barrier: in order to prevent losing replicas at same time that other machines asks for them
            barrier.Leave();
            // update the ZK server!!
            AirlineReplicationModule.Instance.updateMachineData(machines);

            // update the primaries and backups lists - now we can remove the not needed replicas
            primaries.RemoveAll(delegate(Seller p) { return !machines[myName].primaryOf.Contains(p.name); });
            backups.RemoveAll(delegate(Seller p) { return !machines[myName].backsUp.Contains(p.name); });

            // print replicas status
            print(machines);
        }

        private void balanceTheTreeAfterJoined(Dictionary<string, ZNodesDataStructures.MachineNode> machines, Dictionary<string, ZNodesDataStructures.MachineNode> machinesOldVersion, string joinedMachine, Uri machine)
        {
            // calculate the average number of sellers (separately primaries and backups)
            // that each machine has to hold approximately in order to keep load balancing
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            averageP = Convert.ToInt32(Math.Ceiling(Convert.ToDecimal(averageP) / machines.Count));
            averageB = Convert.ToInt32(Math.Ceiling(Convert.ToDecimal(averageB) / machines.Count));

            // this phase handle the primary replicas of sellers in each machine:
            foreach (string busyMachine in getTheMostBusyMachineOrderByPrimaries(machines))
            {

                //if the machine has too many primaries
                while (machines[busyMachine].primaryOf.Count > averageP)
                {

                    // choose a seller in a deterministic way and pass it on
                    machines[busyMachine].primaryOf.Sort();
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();
                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);

                    // the new joined machine has the less primary replicas
                    machines[joinedMachine].primaryOf.Add(primaryToTransfer);

                    // if this machine is the new joined machine - follow the order
                    if (joinedMachine.Equals(myName))
                    {
                        Uri uri = FindPrimaryOfSeller(primaryToTransfer, machinesOldVersion);
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                                                      new BasicHttpBinding(), new EndpointAddress(uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            try
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToPrimary = sellerCluster.sendPrimarySeller(primaryToTransfer);
                                primaries.Add(sellerToPrimary);
                            }
                            catch (FaultException fe)
                            {
                                Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                return;
                            }
                        }
                    }
                }
            }

            // this phase handle the backup replicas of sellers in each machine:
            // this phase works the same as the above BUT there is 1 constrain.
            string lightMachine = getTheMostBusyMachineOrderByBackups(machines).Last();

            foreach (string busyMachine in getTheMostBusyMachineOrderByBackups(machines))
            {
                while (machines[busyMachine].backsUp.Count > averageB)
                {

                    // sort the backups in  a deterministic (stable) way - but thow out
                    // the sellers that the new machine owns as primary
                    List<string> busyMachinesBackups = new List<string>(machines[busyMachine].backsUp).Except(machines[lightMachine].primaryOf).ToList();
                    busyMachinesBackups = busyMachinesBackups.Except(machines[lightMachine].backsUp).ToList();
                    busyMachinesBackups.Sort();

                    // if there are no potential backups - look at the next machine
                    if (busyMachinesBackups.Count == 0) break;
                    string BackupToTransfer = busyMachinesBackups.First();
                    machines[busyMachine].backsUp.Remove(BackupToTransfer);
                    machines[lightMachine].backsUp.Add(BackupToTransfer);
                    if (lightMachine.Equals(myName))
                    {

                        Uri uri = FindPrimaryOfSeller(BackupToTransfer, machinesOldVersion);
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                            new BasicHttpBinding(), new EndpointAddress(uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            try
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToBackup = sellerCluster.sendPrimarySeller(BackupToTransfer);
                                backups.Add(sellerToBackup);
                            }
                            catch (FaultException fe)
                            {
                                Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                return;
                            }
                        }
                    }
                }
            }
        }
        /*
        private Dictionary<string, ZNodesDataStructures.MachineNode> copyMachines(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machinesClone = new Dictionary<string, ZNodesDataStructures.MachineNode>();
            foreach (string name in machines.Keys)
            {
                machinesClone.Add(name, new ZNodesDataStructures.MachineNode());
                machinesClone[name].uri = machines[name].uri;
                machinesClone[name].backsUp = new List<string>(machines[name].backsUp);
                machinesClone[name].primaryOf = new List<string>(machines[name].primaryOf);
            }
            return machinesClone;
        }*/

        private void balanceTheTreeAfterLeft(Dictionary<string, ZNodesDataStructures.MachineNode> machines, Dictionary<string, ZNodesDataStructures.MachineNode> machinesOldView, List<string> backupsToAssign, List<string> primariesToAssign)
        {

            // calculate the average number of sellers (separately primaries and backups)
            // that each machine has to hold approximately in order to keep load balancing
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            averageP = Convert.ToInt32(Math.Ceiling((Convert.ToDecimal(averageP) + primariesToAssign.Count) / machines.Count));
            averageB = Convert.ToInt32(Math.Ceiling((Convert.ToDecimal(averageB) + backupsToAssign.Count) / machines.Count));

            // we want a deterministic list of the machines order by their availability
            // so we reverese the list
            List<string> reverseBusy = getTheMostBusyMachineOrderByPrimaries(machines);
            reverseBusy.Reverse();
            primariesToAssign.Sort();
            List<string> backupsThatShouldBeMoved = new List<string>();
            foreach (string lightMachine in reverseBusy)
            {
                // no primaries to assign
                if (primariesToAssign.Count == 0) break;

                // move primaries to the light machine
                while (machines[lightMachine].primaryOf.Count < averageP)
                {
                    if (primariesToAssign.Count == 0) break;

                    // do the transfer
                    string primaryToTransfer = primariesToAssign.First();
                    machines[lightMachine].primaryOf.Add(primaryToTransfer);
                    if (machines[lightMachine].backsUp.Contains(primaryToTransfer))
                    {
                        machines[lightMachine].backsUp.Remove(primaryToTransfer);
                        backupsToAssign.Add(primaryToTransfer);
                        backupsThatShouldBeMoved.Add(primaryToTransfer);
                    }

                    // ask for the seller from another machine
                    if (lightMachine.Equals(myName))
                    {

                        Uri uri = FindBackupOfSeller(primaryToTransfer, machinesOldView);
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            try
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToPrimary = sellerCluster.sendBackupSeller(primaryToTransfer);
                                primaries.Add(sellerToPrimary);
                            }
                            catch (FaultException fe)
                            {
                                Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                return;
                            }
                        }
                    }
                    primariesToAssign.Remove(primaryToTransfer);
                }
            }

            // this phase load balance the backup
            List<string> idles = getTheMostBusyMachineOrderByBackups(machines);
            idles.Reverse();
            backupsToAssign.Sort();

            // balance the machines order by backups
            foreach (string lightMachine in idles)
            {
                if (backupsToAssign.Count == 0) break;

                // move until machine get to the average amount of backups
                while (machines[lightMachine].backsUp.Count < averageB)
                {
                    if (backupsToAssign.Except(machines[lightMachine].primaryOf).Count() == 0) break;
                    List<string> bs = backupsToAssign.Except(machines[lightMachine].primaryOf).ToList();
                    bs.Sort();
                    string backupToTransfer = bs.First();
                    machines[lightMachine].backsUp.Add(backupToTransfer);

                    // we distinct between 2 cases: backups that have been removed from another machine, and backups that
                    // came from the dropped machine
                    if (lightMachine.Equals(myName))
                    {
                        if (backupsThatShouldBeMoved.Contains(backupToTransfer))
                        {
                            Uri uri = FindBackupOfSeller(backupToTransfer, machinesOldView);
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                try
                                {
                                    Seller sellerToPrimary;
                                    ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                    sellerToPrimary = sellerCluster.sendBackupSeller(backupToTransfer);
                                    backups.Add(sellerToPrimary);
                                }
                                catch (FaultException fe)
                                {
                                    Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                    return;
                                }
                            }
                        }
                        else
                        {
                            Uri uri = FindPrimaryOfSeller(backupToTransfer, machinesOldView);
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                try
                                {
                                    Seller sellerToPrimary;
                                    ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                    sellerToPrimary = sellerCluster.sendPrimarySeller(backupToTransfer);
                                    backups.Add(sellerToPrimary);
                                }
                                catch (FaultException fe)
                                {
                                    Console.WriteLine("Failure in getting seller. Because: " + fe.Message);
                                    return;
                                }
                            }
                        }
                    }
                    backupsToAssign.Remove(backupToTransfer);
                }
            }

        }
        private Uri FindPrimaryOfSeller(string sellerName,Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            return (from m in machines where m.Value.primaryOf.Contains(sellerName) select m.Value.uri).First();
        }

        private Uri FindBackupOfSeller(string sellerName, Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            return (from m in machines where m.Value.backsUp.Contains(sellerName) select m.Value.uri).First();
        }
        public void respondIfSomeoneLeft(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup)
        {
            var barrier = AirlineReplicationModule.Instance.barrier();
            // If this machine is a leader: register as a delegate
            if (!isLeader && checkIfLeader())
            {
                isLeader = true;
                try
                {
                    ServiceEndpoint sellerSearchEndPoint =
                    new ServiceEndpoint(ContractDescription.GetContract(
                        typeof(IAirSellerRegisteration)),
                        new WebHttpBinding(),
                        new EndpointAddress
                        (mysearchServerAddress));
                    using (WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(sellerSearchEndPoint))
                    {
                        IAirSellerRegisteration registerChannel = cf.CreateChannel();
                        //Register the channel in the server
                        registerChannel.RegisterSeller(new Uri(myAddress), clusterName);
                    }
                }
                catch (ProtocolException e)
                {
                    Console.WriteLine("Bad Protocol: " + e.Message);
                }
                catch (Exception e)
                {

                    if (e.InnerException is WebException)
                    {
                        HttpWebResponse resp = (HttpWebResponse)((WebException)e.InnerException).Response;
                        Console.WriteLine("Failed, {0}", resp.StatusDescription);
                    }
                    else
                    {
                        Console.WriteLine("Advertisement connection kicked the bucket, quitting because:");
                        Console.WriteLine(e.Message.ToString());
                    }
                    return;
                }
            }

            // get the updated snapshot
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = AirlineReplicationModule.Instance.Machines;
            Dictionary<string, ZNodesDataStructures.MachineNode> machinesOldVersion = new Dictionary<string, ZNodesDataStructures.MachineNode>(machines);

            Console.WriteLine("\t** BALANCING ALGORITHM STARTED **");
            // execute a deterministic load-balancing algorithm - and also fill the machines with backup nodes 
            balanceTheTreeAfterLeft(machines, machinesOldVersion, sellersWhoLostBackup, sellersWhoLostPrimary);
            Console.WriteLine("\t** BALANCING ALGORITHM FINISHED **");


            //Barrier
            barrier.Leave();

            AirlineReplicationModule.Instance.updateMachineData(machines);

            primaries.RemoveAll(delegate(Seller s) { return !machines[myName].primaryOf.Contains(s.name); });

            backups.RemoveAll(delegate(Seller s) { return !machines[myName].backsUp.Contains(s.name); });

            print(machines);
        }

        public List<Flight> getRelevantFlightsBySrc(string src, DateTime date, List<string> sellersToSearch)
        {
            List<Flight> relevantFlights = new List<Flight>();
            List<Seller> primariesToSearch;
            if (sellersToSearch.Count == 0)
            {
                primariesToSearch = primaries;
            }
            else
            {
                primariesToSearch = primaries.Where(delegate(Seller s) { return sellersToSearch.Contains(s.name); }).ToList();
            }
            foreach (Seller airline in primariesToSearch)
            {
                foreach (Flight flight in airline.flights)
                {
                    if (flight.src.Equals(src) && flight.date.Equals(date))
                    {
                        relevantFlights.Add(flight);
                    }
                }
            }
            return relevantFlights;
        }

        public List<Flight> getRelevantFlightsByDst(string dst, DateTime date, List<string> sellersToSearch)
        {
            List<Flight> relevantFlights = new List<Flight>();

            List<Seller> primariesToSearch;
            if (sellersToSearch.Count == 0)
            {
                primariesToSearch = primaries;
            }
            else
            {
                primariesToSearch = primaries.Where(delegate(Seller s) { return sellersToSearch.Contains(s.name); }).ToList();
            }
            foreach (Seller airline in primariesToSearch)
            {
                foreach (Flight flight in airline.flights)
                {
                    if (flight.dst.Equals(dst) && flight.date.Equals(date))
                    {
                        relevantFlights.Add(flight);
                    }
                }
            }
            return relevantFlights;
        }

        public Seller sendPrimarySeller(string sellerName)
        {
            return primaries.Where(delegate(Seller s) { return s.name.Equals(sellerName); }).First();
        }

        public Seller sendBackupSeller(string sellerName)
        {
            return backups.Where(delegate(Seller s) { return s.name.Equals(sellerName); }).First();
        }

        public void print(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            Console.Write("my primaries: ");
            foreach (Seller s in primaries)
            {
                Console.Write(s.name+", ");
            }
            Console.WriteLine();
            Console.Write("my backups: ");
            foreach (Seller s in backups)
            {
                Console.Write(s.name + ", ");
            }
            Console.WriteLine();
        }
    }
}
