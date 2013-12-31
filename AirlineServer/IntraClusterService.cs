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
        string myName = null;
        string mysearchServerAddress;
        string myAddress;
        string clusterName;
        object locker;

        public IntraClusterService(Seller initialSeller, string searchServerAddress, string thisAddress, string clusterName, object lockObject)
        {
            primaries = new List<Seller>();
            primaries.Add(initialSeller);
            backups = new List<Seller>();
            //checkIfLeader();
            
            mysearchServerAddress= searchServerAddress;
            myAddress = thisAddress;
            this.clusterName = clusterName;
            locker = lockObject;
        }

        public void setName(string name)
        {
            myName = name;
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
            var machines = AirlineReplicationModule.Instance.Machines;
            var keys = machines.Keys;
            return isLeader = myName.Equals(keys.Min());
        }

        public void respondIfNewNode(String machineName, String sellerName, Uri machine )
        {

            ZKSynch barrier = AirlineReplicationModule.Instance.barrier(); //Barrier - Create, Enter, BLOCK -> Leave when Balancing ends
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
            if(!machineName.Equals(myName)){
                primaries.RemoveAll(delegate(Seller candidate){
                    return candidate.name.Equals(sellerName);
                });
                
                backups.RemoveAll(delegate(Seller candidate){
                    return candidate.name.Equals(sellerName);
                });
            }

            // the new machine holds the new seller
            // TODO: make sure that Doron is doint it!!
            //machines[sellerName].primaryOf.Add(sellerName);

            // If there is only a single machine there is no need to backup the seller and load balancing
            if (machines.Count == 1)
            {
                try
                {
                   // WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(new Uri(mysearchServerAddress));
                   // IAirSellerRegisteration registerChannel = cf.CreateChannel();
                    // Register the channel in the server
                    //registerChannel.RegisterSeller(new Uri(myAddress), clusterName);
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
            }else
            {
                // if there are 2 machines - it means that before the current machine has joined there were no backups!
                if (machines.Count == 2)
                {
                    string oldMachine = machines.Keys.Where(delegate(string str)
                    {
                        return !str.Equals(machineName);
                    }).First();
                    foreach (string mac in machines.Keys)
                    {
                        if (!mac.Equals(myName))
                        {
                            foreach (string sell in machines[mac].primaryOf)
                            {
                                ServiceEndpoint endPoint = new ServiceEndpoint(ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[mac].uri));
                                using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                                {
                                    Console.WriteLine("uri: {0}, machine Name: [{1}]", machines[mac].uri, machineName);
                                    ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                                    Seller sellerToBackup = sellerCluster.sendPrimarySeller(sell);
                                    backups.Add(sellerToBackup);
                                }
                            }

                        }
                    }
                    if (!myName.Equals(oldMachine))
                    {
                        foreach (string s in machines[oldMachine].primaryOf)
                        {
                            machines[myName].backsUp.Add(s);
                        }
                    }
                }
                // find a deterministic victim to hold the backup of the new seller
                string victim = (from p in machines where !p.Key.Equals(sellerName) orderby p.Value.primaryOf.Count ascending, p.Key ascending select p.Key).First();

                // if the victim is this machine - follow the command and ask a clone from the new machine that owns the primary replica
                if (victim.Equals(myName))
                {
                    ServiceEndpoint endPoint = new ServiceEndpoint(
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[machineName].uri));
                    using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                    {
                        ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                        Seller sellerToBackup = sellerCluster.sendPrimarySeller(sellerName);
                        backups.Add(sellerToBackup);
                    }
                }

                // update it in the ZK tree
                machines[victim].backsUp.Add(sellerName);

                // execute a deterministic load-balancing algorithm
                balanceTheTreeAfterJoined(machines, machineName, machine);
            }
            print(machines);
            // this lock makes sure that no search server will serviced while sellers are removed from the machine.
            //lock (locker)
            //{
            Console.WriteLine("\t** BALANCING ALGORITHM FINISHED **");
            barrier.Leave();
            Console.WriteLine("\t** OOOOOOOOOUUUUUUUUUUTTTTTTTT");
                //Console.WriteLine("IIIIIIIINNNNNNNN");
                //// barrier: in order to prevent losing replicas at same time that other machines asks for them
                //AirlineReplicationModule.Instance.barrier();//Barrier
                //Console.WriteLine("OOOOOOOOOUUUUUUUUUUTTTTTTTT");
                // update the ZK server!!
                AirlineReplicationModule.Instance.updateMachineData(machines[myName]);

                // update the primaries and backups lists
                primaries.RemoveAll(delegate(Seller p) { return !machines[myName].primaryOf.Contains(p.name); });

                backups.RemoveAll(delegate(Seller p) { return !machines[myName].backsUp.Contains(p.name); });
           // }

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
            foreach(string busyMachine in getTheMostBusyMachineByPrimaries(machines)){
                
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

            // this phase handle the backup replicas of sellers in each machine:
            // this phase works the same as the above BUT there is 1 constrain.
            foreach(string busyMachine in getTheMostBusyMachineByBackups(machines)){
                while(machines[busyMachine].backsUp.Count >  averageB){
                    // sort the backups in  a deterministic (stable) way - but thow out
                    // the sellers that the new machine owns as primary
                    List<string> busyMachinesBackups = new List<string>(machines[busyMachine].backsUp).Except(machines[joinedMachine].primaryOf).ToList();
                    busyMachinesBackups.Sort();

                    string BackupToTransfer = busyMachinesBackups.First();
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
            List<string> reverseBusy = getTheMostBusyMachineByPrimaries(machines);
            reverseBusy.Reverse();


            foreach (string busyMachine in getTheMostBusyMachineByPrimaries(machines))
            {
                while (machines[busyMachine].primaryOf.Count >  averageP)
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
                bool isSet = false;
                foreach (string idleMachine in idles)
                {
                    while (machines[idleMachine].backsUp.Count < averageB && !machines[idleMachine].primaryOf.Contains(backupToAssign))
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
                        isSet = true;
                        break;
                    }
                    if (isSet) break;
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
            // If this machine is a leader: register as a delegate
            if (checkIfLeader())
            {
                      try
                        {
                           // WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(new Uri(mysearchServerAddress));
                          //  IAirSellerRegisteration registerChannel = cf.CreateChannel();
                            // Register the channel in the server
                          //  registerChannel.RegisterSeller(new Uri(myAddress), clusterName);
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
            if (machines.Count > 1)
            {
                // execute a deterministic load-balancing algorithm - and also fill the machines with backup nodes 
                balanceTheTreeAfterLeft(machines, sellersWhoLostPrimary.Concat(sellersWhoLostBackup).ToList());
            }
            else
            {
                
            }

            print(machines);

            //lock (locker)
           // {
                Console.WriteLine("IN!!!!!!!!!!!!!!");
                AirlineReplicationModule.Instance.barrier();//Barrier
                Console.WriteLine("OUT!!!!!!!!!");
                AirlineReplicationModule.Instance.updateMachineData(machines[myName]);

                primaries.RemoveAll(delegate(Seller s) { return !machines[myName].primaryOf.Contains(s.name); });

                backups.RemoveAll(delegate(Seller s) { return !machines[myName].backsUp.Contains(s.name); });
          //  }

        }

        private void setMachineAsPrimary(List<string> sellerNames)
        {
                      // Update it in the ZK tree
            foreach (Seller m in backups)
            {
                if (sellerNames.Contains(m.name)) primaries.Add(m);
            }
                backups.RemoveAll(delegate(Seller s) { return sellerNames.Contains(s.name); });
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

        public void print(Dictionary<string, ZNodesDataStructures.MachineNode> machines)
        {
            foreach (string k in machines.Keys)
            {
                Console.WriteLine(k+": ");
                Console.Write("primaries: ");
                foreach (string pr in machines[k].primaryOf)
                {
                    Console.Write(pr+", ");
                }
                Console.WriteLine();
                Console.Write("backups: ");
                foreach (string ba in machines[k].backsUp)
                {
                    Console.Write(ba + ", ");
                }
                Console.WriteLine();
            }
        }
    }
}
