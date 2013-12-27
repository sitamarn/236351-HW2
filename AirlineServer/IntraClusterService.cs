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
        List<AirlineServer.Seller> primaries;
        List<AirlineServer.Seller> backups;
        bool isLeader  = false;
        string myName;
        string mysearchServerAddress;
        string myAddress;
        string clusterName;

        public IntraClusterService(Seller initialSeller, string name, string searchServerAddress, string thisAddress, string clusterName)
        {
            primaries = new List<Seller>();
            primaries.Add(initialSeller);
            backups = new List<Seller>();
            checkIfLeader();
            myName = name;
            mysearchServerAddress= searchServerAddress;
            myAddress = thisAddress;
            this.clusterName = clusterName;
        }

        private List<string> getTheMostBusyMachineByPrimaries(Dictionary<string, ZNodesDataStructures.MachineNode> machines){
            //Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();

            return (from p in machines orderby p.Value.primaryOf.Count descending, p.Key ascending select p.Key ).ToList();
        }

        private List<string> getTheMostBusyMachineByBackups(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            //Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();

            return (from p in machines orderby p.Value.backsUp.Count descending, p.Key ascending select p.Key).ToList();
        }

        private bool checkIfLeader()
        {
            string minMachine = null; // TODO: fix me TreeViewLib.TreeView.Machines.Machines.Keys.Min();
            //string myID = TreeViewLib.TreeView.Instance.myID;
            return isLeader = myName.Equals(minMachine);
        }

        public void respondIfNewNode(String machineName, String sellerName, Uri machine )
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = null;// TODO: FIX ME Machines();
            foreach(string smachine in machines.Keys){
                machines[smachine].primaryOf.Remove(sellerName);
                machines[smachine].backsUp.Remove(sellerName);   
            }
            if(!machineName.Equals(myName)){
                primaries.RemoveAll(delegate(Seller candidate){
                    return candidate.name.Equals(sellerName);
                });
                
                backups.RemoveAll(delegate(Seller candidate){
                    return candidate.name.Equals(sellerName);
                });
            }

            machines[sellerName].primaryOf.Add(sellerName);
            string victim = (from p in machines where !p.Key.Equals(sellerName) orderby p.Value.primaryOf.Count ascending,p.Key ascending select p.Key).First();
            if(victim.Equals(myName)){
                ServiceEndpoint endPoint = new ServiceEndpoint(
                    ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[machineName].uri));
                using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                {
                    ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                    Seller sellerToBackup = sellerCluster.sendPrimarySeller(sellerName);
                    backups.Add(sellerToBackup);
                }
            }
            machines[victim].backsUp.Add(sellerName);

            balanceTheTreeAfterJoined(machines,machineName, machine);


        }
        
        private void balanceTheTreeAfterJoined(Dictionary<string, ZNodesDataStructures.MachineNode> machines,string joinedMachine, Uri machine)
        {
            //Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();
            //Dictionary<string, int> balancesP = new Dictionary<string, int>();
            //Dictionary<string, int> balancesB = new Dictionary<string, int>();
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            averageP = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageP) / machines.Count));
            averageB = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageB) / machines.Count));
            foreach(string busyMachine in getTheMostBusyMachineByPrimaries(machines)){
                while(machines[busyMachine].primaryOf.Count > 1+ averageP){
                    machines[busyMachine].primaryOf.Sort();
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();
                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);
                    machines[joinedMachine].primaryOf.Add(primaryToTransfer);
                    if(joinedMachine.Equals(myName)){
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                            new BasicHttpBinding(), new EndpointAddress(machines[busyMachine].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToPrimary = sellerCluster.sendPrimarySeller(primaryToTransfer);
                            primaries.Add(sellerToPrimary);
                        }
                    }
                }
            }

            foreach(string busyMachine in getTheMostBusyMachineByBackups(machines)){
                while(machines[busyMachine].backsUp.Count > 1+ averageB){
                    List<string> clone = new List<string>(machines[busyMachine].backsUp).Except(machines[joinedMachine].primaryOf).ToList();
                    clone.Sort();
                    string BackupToTransfer = clone.First();

                    machines[busyMachine].backsUp.Remove(BackupToTransfer);
                    machines[joinedMachine].backsUp.Add(BackupToTransfer);
                    if(joinedMachine.Equals(myName)){
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                            new BasicHttpBinding(), new EndpointAddress(machines[busyMachine].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToBackup = sellerCluster.sendBackupSeller(BackupToTransfer);
                            backups.Add(sellerToBackup);
                        }
                    }
                }
            }


            //Barrier
            //updateTree(myName,machines[myName].primaryOf, machines[myName].backsUp);
            foreach (Seller prim in primaries)
            {
                if (!machines[myName].primaryOf.Contains(prim.name)) primaries.Remove(prim);
            }

            foreach (Seller prim in backups)
            {
                if (!machines[myName].backsUp.Contains(prim.name)) backups.Remove(prim);
            }
        }

        private void balanceTheTreeAfterLeft(Dictionary<string, ZNodesDataStructures.MachineNode> machines, List<string> backupsToAssign)
        {
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            List<string> reverseBusy = getTheMostBusyMachineByPrimaries(machines);
            reverseBusy.Reverse();
            averageP = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageP) / machines.Count));
            averageB = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageB) / machines.Count));
            foreach (string busyMachine in getTheMostBusyMachineByPrimaries(machines))
            {
                while (machines[busyMachine].primaryOf.Count > 1 + averageP)
                {
                    machines[busyMachine].primaryOf.Sort();
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();

                    string victim = null;
                    foreach (string idle in reverseBusy)
                    {
                        if (machines[idle].primaryOf.Count < averageP && !machines[idle].backsUp.Contains(primaryToTransfer))
                        {
                            victim = idle;
                            break;
                        }
                    }
                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);
                    machines[victim].primaryOf.Add(primaryToTransfer);
                    if (victim.Equals(myName))
                    {
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),new BasicHttpBinding(), new EndpointAddress(machines[busyMachine].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToPrimary = sellerCluster.sendBackupSeller(primaryToTransfer);
                            primaries.Add(sellerToPrimary);
                        }
                    }
                }
            }

            List<string> idles = getTheMostBusyMachineByBackups(machines);
            idles.Reverse();
            backupsToAssign.Sort();
            foreach (string backupToAssign in backupsToAssign)
            {
                foreach (string idleMachine in idles)
                {
                    while (machines[idleMachine].backsUp.Count < 1 + averageB && !machines[idleMachine].primaryOf.Contains(backupToAssign))
                    {

                        //machines[busyMachine].backsUp.Remove(BackupToTransfer);
                        machines[idleMachine].backsUp.Add(backupToAssign);
                        if (idleMachine.Equals(myName))
                        {
                            Uri uri = FindPrimaryOfSeller(backupToAssign,machines);
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToBackup = sellerCluster.sendPrimarySeller(backupToAssign);
                                primaries.Add(sellerToBackup);
                            }
                        }
                    }
                }
            }
        }
        private Uri FindPrimaryOfSeller(string sellerName, Dictionary<string, ZNodesDataStructures.MachineNode> machines){
            foreach (string machineName in machines.Keys)
            {
                if (machines[machineName].primaryOf.Contains(sellerName))
                {
                    return machines[machineName].uri;
                }
            }
            return null;
        }
        public void respondIfSomeoneLeft(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup)
        {
            if (checkIfLeader())
            {
                      try
                        {
                            WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(new Uri(mysearchServerAddress));
                            IAirSellerRegisteration registerChannel = cf.CreateChannel();
                            // Register the channel in the server
                            registerChannel.RegisterSeller(new Uri(myAddress), clusterName);
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

            Dictionary<string, ZNodesDataStructures.MachineNode> machines = null;// TODO: fix me Machines();
            foreach (Seller seller in backups)
            {
                if(sellersWhoLostPrimary.Contains(seller.name)){
                    setMachineAsPrimary(seller.name);
                }
            }

            
            foreach (string m in machines.Keys)
            {

                foreach (string seller in machines[m].backsUp)
                {
                    if (sellersWhoLostPrimary.Contains(seller))
                    {
                        machines[m].primaryOf.Add(seller);
                        machines[m].backsUp.Remove(seller);
                    }
                    
                }
            }




            // TODO: Update the tree

            foreach (Seller prim in primaries)
            {
                if (!machines[myName].primaryOf.Contains(prim.name)) primaries.Remove(prim);
            }

            foreach (Seller prim in backups)
            {
                if (!machines[myName].backsUp.Contains(prim.name)) backups.Remove(prim);
            }





        }

        private void setMachineAsPrimary(string sellerName)
        {
            foreach (AirlineServer.Seller backup in backups)
            {
                if (backup.name.Equals(sellerName))
                {
                    primaries.Add(backup);
                    backups.Remove(backup);
                }
            }
        }


        public List<Flight> getRelevantFlightsBySrc(string src, DateTime date)
        {
            List<Flight> relevantFlights = new List<Flight>();
            foreach (Seller airline in primaries)
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

        public List<Flight> getRelevantFlightsByDst(string dst, DateTime date)
        {
            List<Flight> relevantFlights = new List<Flight>();
            foreach (Seller airline in primaries)
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
    }
}
