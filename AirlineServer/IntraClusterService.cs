using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Text;
using System.Threading.Tasks;
using TreeViewLib;

namespace AirlineServer
{
    class IntraClusterService: ISellerClusterService,IReplicatedSeller
    {
        List<AirlineServer.Seller> primaries;
        List<AirlineServer.Seller> backups;

        public IntraClusterService(Seller initialSeller)
        {
            primaries = new List<Seller>();
            primaries.Add(initialSeller);
            backups = new List<Seller>();
        }

        private List<string> getTheMostBusyMachineByPrimaries(){
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();

            return (from p in machines orderby p.Value.primaryOf.Count descending select p.Key ).ToList();
        }

        private List<string> getTheMostBusyMachineByBackups()
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();

            return (from p in machines orderby p.Value.backsUp.Count descending select p.Key).ToList();
        }


        
        private void balanceTheTreeAfterJoined(string joinedMachine, Uri machine)
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();
            Dictionary<string, int> balancesP = new Dictionary<string, int>();
            Dictionary<string, int> balancesB = new Dictionary<string, int>();
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            averageP = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageP) / machines.Count));
            averageB = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageB) / machines.Count));
            foreach(string machineName in getTheMostBusyMachineByPrimaries()){
                while(machines[machineName].primaryOf.Count > 1+ averageP){
                Uri busyUri = machines[machineName].uri;
                //TODO: SOME SOAP CODE

                //TODO: SOME UPDATDING TREE CODE
                    continue;
                }
                break;
            }

            
            
            foreach(string machineName in getTheMostBusyMachineByBackups()){
                if(machines[machineName].backsUp.Count > 1+ averageP){
                Uri busyUri = machines[machineName].uri;
                   foreach(string backup in machines[machineName].backsUp){
                       while(machines[machineName].backsUp.Count > 1+ averageP){
                       if(!machines[joinedMachine].primaryOf.Contains(backup)){
                           Uri busyuri = machines[machineName].uri;
                           //TODO: SOME SOAP CODE
                           //TODO: SOME UPDATDING TREE CODE         
                       }
                   }
                   }


                    continue;
                }
                break;
            }
        }

        public void setMachineAsPrimary(string sellerName)
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

        public void setMachineAsBackup(string sellerName)
        {

              ServiceEndpoint endPoint = new ServiceEndpoint(
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(TreeViewLib.TreeView.Machines.Snapshot[sellerName]));
                    using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                    {
                        ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                        Seller sellerToBackup = sellerCluster.getSellerClone(sellerName);

                        backups.Add(sellerToBackup);
                    }

        }

        public void resetCache(string sellerName)
        {
            throw new NotImplementedException();
        }

        public void dropSeller(string sellerName)
        {
            foreach (Seller primary in primaries)
            {
                if (primary.name.Equals(sellerName))
                {
                    primaries.Remove(primary);
                    return;
                }
            }

            foreach (Seller backup in backups)
            {
                if (backup.name.Equals(sellerName))
                {
                    backups.Remove(backup);
                    return;
                }
            }
        }

        public Dictionary<string, int> getSellersAndTheirVersions()
        {
            return null;
        }

        public Seller getSellerClone(string seller)
        {
            foreach (Seller primary in primaries)
            {
                if (primary.name.Equals(seller))
                {
                    Seller sellerClone = new Seller();
                    sellerClone.name = primary.name;
                    sellerClone.flights = new List<Flight>(primary.flights);
                    return sellerClone;
                }
            }
            return null;
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
    }
}
