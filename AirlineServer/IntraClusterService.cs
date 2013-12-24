using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class IntraClusterService: ISellerClusterService,IReplicatedSeller
    {
        List<AirlineServer.ISellerService.Seller> primaries;
        List<AirlineServer.ISellerService.Seller> backups;

        public IntraClusterService(ISellerService.Seller initialSeller)
        {
            primaries = new List<ISellerService.Seller>();
            primaries.Add(initialSeller);
            backups = new List<ISellerService.Seller>();
        }

        public void setMachineAsPrimary(string sellerName)
        {
            foreach (AirlineServer.ISellerService.Seller backup in backups)
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

        public ISellerService.Seller getSellerClone(string seller)
        {
            throw new NotImplementedException();
        }

        public List<ISellerService.Flight> getRelevantFlightsBySrc(string src, DateTime date)
        {
            throw new NotImplementedException();
        }

        public List<ISellerService.Flight> getRelevantFlightsByDst(string dst, DateTime date)
        {
            throw new NotImplementedException();
        }
    }
}
