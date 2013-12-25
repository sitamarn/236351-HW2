using System;
using System.Collections.Generic;
using System.Linq;
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
            throw new NotImplementedException();
        }

        public void resetCache(string sellerName)
        {
            throw new NotImplementedException();
        }

        public void dropSeller(string sellerName)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, int> getSellersAndTheirVersions()
        {
            throw new NotImplementedException();
        }

        public Seller getSellerClone(string seller)
        {
            throw new NotImplementedException();
        }

        public List<Flight> getRelevantFlightsBySrc(string src, DateTime date)
        {

            throw new NotImplementedException();
        }

        public List<Flight> getRelevantFlightsByDst(string dst, DateTime date)
        {
            throw new NotImplementedException();
        }
    }
}
