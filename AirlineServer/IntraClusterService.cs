using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Text;
using System.Threading.Tasks;

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
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(TreeViewLib.TreeView.Instance.Snapshot[sellerName]));
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
