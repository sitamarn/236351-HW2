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

        public void respondIfNewNode(String machineName, String sellerName, Uri machine )
        {
            // Clear the cache because the system changed -> the data changed
            cache.clear();

            //Barrier - Create, Enter, BLOCK -> Leave when Balancing ends
            var barrier = AirlineReplicationModule.Instance.barrier();

            // Get the cuurent snapshot of the system - including the new machine that came up!
            //not included the sellers that it holds.
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = AirlineReplicationModule.Instance.Machines;

            // in the ZK tree: If a machine holds a seller that has return to live - drop it!
            foreach(string smachine in machines.Keys){
                if (!smachine.Equals(machineName))
                {
                machines[smachine].primaryOf.Remove(sellerName);
                machines[smachine].backsUp.Remove(sellerName);  
                }
            }

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
                            try
                            {
                                ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[oldMachine].uri));
                                using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                                {
                                    ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                    Seller sellerToBackup = sellerCluster.sendPrimarySeller(sell);
                                    if (sellerToBackup == null)
                                    {
                                        sellerToBackup = sellerCluster.sendBackupSeller(sellerName);
                                    }
                                    backups.Add(sellerToBackup);

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

                    }
                }

                // find a deterministic victim to hold the backup of the new seller
                string victim = (from p in machines where !p.Key.Equals(machineName) orderby p.Value.primaryOf.Count ascending, p.Key ascending select p.Key).First();

                // if the victim is this machine - follow the command and ask a clone from the new machine that owns the primary replica
                if (victim.Equals(myName))
                {

                    Uri uri = FindPrimaryOfSeller(sellerName);
                    try
                    {
                        ServiceEndpoint endPoint = new ServiceEndpoint(
                            ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[machineName].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToBackup = sellerCluster.sendPrimarySeller(sellerName);
                            if (sellerToBackup == null)
                            {
                                sellerToBackup = sellerCluster.sendBackupSeller(sellerName);
                            }
                            backups.Add(sellerToBackup);

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
                // update it in the ZK tree
                machines[victim].backsUp.Add(sellerName);


            }

            // execute a deterministic load-balancing algorithm
            if (machines.Keys.Count > 2)
            {
                Console.WriteLine("\t** BALANCING ALGORITHM STARTED **");
                balanceTheTreeAfterJoined(machines, machineName, machine);
            }

            // this lock makes sure that no search server will serviced while sellers are removed from the machine.
            Console.WriteLine("\t** BALANCING ALGORITHM FINISHED **");
            barrier.Leave();

                //// barrier: in order to prevent losing replicas at same time that other machines asks for them

                // update the ZK server!!
                AirlineReplicationModule.Instance.updateMachineData(machines);

                // update the primaries and backups lists - now we can remove the not needed replicas
                primaries.RemoveAll(delegate(Seller p) { return !machines[myName].primaryOf.Contains(p.name);});
                backups.RemoveAll(delegate(Seller p) { return !machines[myName].backsUp.Contains(p.name);  });

                // print replicas status
                print(machines);
        }
        
        private void balanceTheTreeAfterJoined(Dictionary<string, ZNodesDataStructures.MachineNode> machines,string joinedMachine, Uri machine)
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
            foreach(string busyMachine in getTheMostBusyMachineOrderByPrimaries(machines)){
                
                //if the machine has too many primaries
                while(machines[busyMachine].primaryOf.Count >  averageP){

                    // choose a seller in a deterministic way and pass it on
                    machines[busyMachine].primaryOf.Sort();
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();
                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);

                    // the new joined machine has the less primary replicas
                    machines[joinedMachine].primaryOf.Add(primaryToTransfer);

                    // if this machine is the new joined machine - follow the order
                    if(joinedMachine.Equals(myName)){
                        Uri uri = FindPrimaryOfSeller(primaryToTransfer);
                        try
                        {
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                                new BasicHttpBinding(), new EndpointAddress(uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToPrimary = sellerCluster.sendPrimarySeller(primaryToTransfer);
                                if (sellerToPrimary == null) { sellerToPrimary = sellerCluster.sendBackupSeller(primaryToTransfer); }
                                primaries.Add(sellerToPrimary);
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
                                Console.WriteLine("Advertisement connection kicked the bucket, quitting because:{0},{1}", primaryToTransfer, uri.AbsoluteUri);
                                Console.WriteLine(e.Message.ToString());
                            }
                            return;
                        }
                    }
                }
            }

            // this phase handle the backup replicas of sellers in each machine:
            // this phase works the same as the above BUT there is 1 constrain.
            string lightMachine = getTheMostBusyMachineOrderByBackups(machines).Last();

            foreach(string busyMachine in getTheMostBusyMachineOrderByBackups(machines)){
                while(machines[busyMachine].backsUp.Count >  averageB){

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
                        try
                        {
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),
                                new BasicHttpBinding(), new EndpointAddress(machines[busyMachine].uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToBackup = sellerCluster.sendBackupSeller(BackupToTransfer);
                                if (sellerToBackup == null)
                                {
                                    sellerToBackup = sellerCluster.sendPrimarySeller(BackupToTransfer);
                                }
                                backups.Add(sellerToBackup);
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
                                Console.WriteLine("Advertisement connection kicked the bucket, quitting because111:");
                                Console.WriteLine(e.Message.ToString());
                            }
                            return;
                        }
                    }
                }
            }
        }

        private void balanceTheTreeAfterLeft(Dictionary<string, ZNodesDataStructures.MachineNode> machines, List<string> backupsToAssign)
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
            averageB = Convert.ToInt32(Math.Ceiling((Convert.ToDecimal(averageB) + backupsToAssign.Count) / machines.Count));

            // we want a deterministic list of the machines order by their availability
            // so we reverese the list
            List<string> reverseBusy = getTheMostBusyMachineOrderByPrimaries(machines);
            reverseBusy.Reverse();


            foreach (string busyMachine in getTheMostBusyMachineOrderByPrimaries(machines))
            {
                while (machines[busyMachine].primaryOf.Count >  averageP)
                {

                    machines[busyMachine].primaryOf.Sort();

                    string victim = null;
                    string primaryToTransfer = null;
                    foreach (string transferable in machines[busyMachine].primaryOf)
                    {
                        foreach (string idle in reverseBusy)
                        {
                            if (!idle.Equals(busyMachine) && !machines[idle].backsUp.Contains(transferable))
                            {
                                victim = idle;
                                primaryToTransfer = transferable;
                                break;
                            }
                        }

                    }
                    if (victim == null) break;

                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);
                    machines[victim].primaryOf.Add(primaryToTransfer);
                    if (victim.Equals(myName))
                    {
                        ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)),new BasicHttpBinding(), new EndpointAddress(machines[busyMachine].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToPrimary = sellerCluster.sendBackupSeller(primaryToTransfer);
                            if (sellerToPrimary == null)
                                sellerToPrimary = sellerCluster.sendPrimarySeller(primaryToTransfer);
                            primaries.Add(sellerToPrimary);
                        }
                    }
                }
            }

            List<string> idles = getTheMostBusyMachineOrderByBackups(machines);
            idles.Reverse();
            backupsToAssign.Sort();
            foreach (string backupToAssign in backupsToAssign)
            {
                foreach (string idleMachine in idles)
                {
                    if (/*machines[idleMachine].backsUp.Count < averageB+1 && */!machines[idleMachine].primaryOf.Contains(backupToAssign))
                    {
                        machines[idleMachine].backsUp.Add(backupToAssign);
                        if (idleMachine.Equals(myName))
                        {
                            Uri uri = FindPrimaryOfSeller(backupToAssign);
                            ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(uri));
                            using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                            {
                                
                                ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                Seller sellerToBackup = sellerCluster.sendPrimarySeller(backupToAssign);
                                if (sellerToBackup == null) sellerCluster.sendBackupSeller(backupToAssign);
                                backups.Add(sellerToBackup);
                            }
                        }
                        break;
                    }
                }
            }

        }
        private Uri FindPrimaryOfSeller(string sellerName){
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = AirlineReplicationModule.Instance.Machines;
            foreach (string machineName in machines.Keys)
            {
                if (machines[machineName].primaryOf.Contains(sellerName) && !machineName.Equals(myName))
                {
                    
                    return machines[machineName].uri;
                }
            }
            return null;
        }
        public void respondIfSomeoneLeft(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup)
        {
            cache.clear();
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

            // if a primary seller fall - raise the backup immediately


            setMachineAsPrimary(sellersWhoLostPrimary);



            // Update it in the ZK tree
            foreach (string m in machines.Keys)
            {
                foreach (string seller in machines[m].backsUp)
                {
                    if (sellersWhoLostPrimary.Contains(seller))
                    {
                        machines[m].primaryOf.Add(seller);

                    }
                }

                machines[m].backsUp.RemoveAll(delegate(string s) { return sellersWhoLostPrimary.Contains(s); });
            }

            // relevant only if there are more than 1 machine
            if (machines.Count > 0)
            {
                // execute a deterministic load-balancing algorithm - and also fill the machines with backup nodes 
                balanceTheTreeAfterLeft(machines, sellersWhoLostPrimary.Concat(sellersWhoLostBackup).ToList());
            }

            //Barrier
            barrier.Leave();

            AirlineReplicationModule.Instance.updateMachineData(machines);

            primaries.RemoveAll(delegate(Seller s) { return !machines[myName].primaryOf.Contains(s.name); });

            backups.RemoveAll(delegate(Seller s) { return !machines[myName].backsUp.Contains(s.name); });


        }

        private void setMachineAsPrimary(List<string> sellerNames)
        {
                      // Update it in the ZK tree
            foreach (Seller m in backups)
            {
                if (sellerNames.Contains(m.name)) primaries.Add(m);
            }
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
            foreach (Seller p in primaries)
            {
                if (p.name.Equals(sellerName)) { return p; }
            }
            return null;
        }

        public Seller sendBackupSeller(string sellerName)
        {
            foreach (Seller p in backups)
            {
                if (p.name.Equals(sellerName)) { return p; }
            }
            return null;
        }

        public void print(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            Console.WriteLine("my primaries: ");
            foreach (Seller s in primaries)
            {
                Console.Write(s.name+", ");
            }
            Console.WriteLine();
            Console.WriteLine("my backups: ");
            foreach (Seller s in backups)
            {
                Console.Write(s.name + ", ");
            }
            Console.WriteLine();
        }
    }
}
