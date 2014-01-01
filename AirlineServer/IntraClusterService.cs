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
            //if(machineName.Equals(myName)) primaries.add
            ZKSynch barrier = AirlineReplicationModule.Instance.barrier(); //Barrier - Create, Enter, BLOCK -> Leave when Balancing ends
            // Get the cuurent snapshot of the system - including the new machine that came up!
            //not included the sellers that it holds.
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = AirlineReplicationModule.Instance.Machines;
            Console.WriteLine(72);
            // in the ZK tree: If a machine holds a seller that has return to live - drop it!
            foreach(string smachine in machines.Keys){
                if (!smachine.Equals(machineName))
                {
                machines[smachine].primaryOf.Remove(sellerName);
                machines[smachine].backsUp.Remove(sellerName);  
                }
            }
            Console.WriteLine(83);
            // in the local data: If a this machine holds an old version of the seller - drop it!!
            if(!machineName.Equals(myName)){
                try
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
                catch(Exception e)
                {
                    Console.WriteLine(e);
                }
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
                                    Console.WriteLine("uri: {0}, machine Name: [{1}]", machines[oldMachine].uri, machineName);
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
                Console.WriteLine(180);
                // find a deterministic victim to hold the backup of the new seller
                string victim = (from p in machines where !p.Key.Equals(machineName) orderby p.Value.primaryOf.Count ascending, p.Key ascending select p.Key).First();
                Console.WriteLine(183);
                // if the victim is this machine - follow the command and ask a clone from the new machine that owns the primary replica
                if (victim.Equals(myName))
                {
                    Console.WriteLine(187);
                    Uri uri = FindPrimaryOfSeller(sellerName);
                    try
                    {
                        ServiceEndpoint endPoint = new ServiceEndpoint(
                            ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[machineName].uri));
                        using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                        {
                            Console.WriteLine(195);
                            ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                            Seller sellerToBackup = sellerCluster.sendPrimarySeller(sellerName);
                            if (sellerToBackup == null)
                            {
                                sellerToBackup = sellerCluster.sendBackupSeller(sellerName);
                            }
                            Console.WriteLine(202);
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
                Console.WriteLine(227);
                // update it in the ZK tree
                machines[victim].backsUp.Add(sellerName);
                Console.WriteLine(230);
                // execute a deterministic load-balancing algorithm
                Console.WriteLine("\t** BALANCING ALGORITHM STARTED **");
                balanceTheTreeAfterJoined(machines, machineName, machine);
            }
            //print(machines);
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
                AirlineReplicationModule.Instance.updateMachineData(machines);

                // update the primaries and backups lists
                primaries.RemoveAll(delegate(Seller p) { return !machines[myName].primaryOf.Contains(p.name);});

                backups.RemoveAll(delegate(Seller p) { return !machines[myName].backsUp.Contains(p.name);  });

                print(machines);
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
                    Console.WriteLine("loop primaries");
                    // choose a seller in a deterministic way and pass it on
                    machines[busyMachine].primaryOf.Sort();
                    Console.WriteLine("{0}", busyMachine);
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();
                    Console.WriteLine("{0}", primaryToTransfer);
                    machines[busyMachine].primaryOf.Remove(primaryToTransfer);
                    // the new joined machine has the less primary replicas
                    machines[joinedMachine].primaryOf.Add(primaryToTransfer);
                    Console.WriteLine("{0}", primaryToTransfer);
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
            Console.WriteLine("take care backpps");
            // this phase handle the backup replicas of sellers in each machine:
            // this phase works the same as the above BUT there is 1 constrain.
            string lightMachine = getTheMostBusyMachineByBackups(machines).Last();

            foreach(string busyMachine in getTheMostBusyMachineByBackups(machines)){
                while(machines[busyMachine].backsUp.Count >  averageB){
                    Console.WriteLine("loop backpps");
                    // sort the backups in  a deterministic (stable) way - but thow out
                    // the sellers that the new machine owns as primary
                    List<string> busyMachinesBackups = new List<string>(machines[busyMachine].backsUp).Except(machines[lightMachine].primaryOf).ToList();
                    busyMachinesBackups = busyMachinesBackups.Except(machines[lightMachine].backsUp).ToList();
                    busyMachinesBackups.Sort();
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
            List<string> reverseBusy = getTheMostBusyMachineByPrimaries(machines);
            reverseBusy.Reverse();


            foreach (string busyMachine in getTheMostBusyMachineByPrimaries(machines))
            {
                while (machines[busyMachine].primaryOf.Count >  averageP)
                {
                    Console.WriteLine("loop getTheMostBusyMachineByPrimaries");
                    machines[busyMachine].primaryOf.Sort();
                    string primaryToTransfer = machines[busyMachine].primaryOf.First();

                    string victim = null;
                    foreach (string idle in reverseBusy)
                    {
                        if (/*machines[idle].primaryOf.Count < averageP+1 && */!machines[idle].backsUp.Contains(primaryToTransfer))
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
                            if(sellerToPrimary==null)
                            sellerToPrimary = sellerCluster.sendPrimarySeller(primaryToTransfer);
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
                    while (/*machines[idleMachine].backsUp.Count < averageB+1 && */!machines[idleMachine].primaryOf.Contains(backupToAssign))
                    {

                        //machines[busyMachine].backsUp.Remove(BackupToTransfer);
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
                        isSet = true;
                        break;
                    }
                    if (isSet) break;
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
            ZKSynch barrier = AirlineReplicationModule.Instance.barrier();
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
            if (machines.Count > 0)
            {
                // execute a deterministic load-balancing algorithm - and also fill the machines with backup nodes 
                balanceTheTreeAfterLeft(machines, sellersWhoLostPrimary.Concat(sellersWhoLostBackup).ToList());
            }
            else
            {
                
            }

            

            //lock (locker)
           // {
                Console.WriteLine("IN!!!!!!!!!!!!!!");
                barrier.Leave();//Barrier
                Console.WriteLine("OUT!!!!!!!!!");
                AirlineReplicationModule.Instance.updateMachineData(machines);

                primaries.RemoveAll(delegate(Seller s) { return !machines[myName].primaryOf.Contains(s.name); });

                backups.RemoveAll(delegate(Seller s) { return !machines[myName].backsUp.Contains(s.name); });

                print(machines);
          //  }

        }

        private void setMachineAsPrimary(List<string> sellerNames)
        {
                      // Update it in the ZK tree
            foreach (Seller m in backups)
            {
                if (sellerNames.Contains(m.name)) primaries.Add(m);
            }
               // backups.RemoveAll(delegate(Seller s) { return sellerNames.Contains(s.name); });
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
            foreach (Seller p in primaries)
            {
                if (p.name.Equals(sellerName)) { return p; }
            }
            Console.WriteLine("ERROR!!!!!!!!!!!!!!!!!!!!!!!!: null seller primaries");
            return null;
        }

        public Seller sendBackupSeller(string sellerName)
        {
            foreach (Seller p in backups)
            {
                if (p.name.Equals(sellerName)) { return p; }
            }
            Console.WriteLine("ERROR!!!!!!!!!!!!!!!!!!!!!!!!: null seller backups");
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
