using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class SellerService:ISellerService
    {
        public List<AirlineServer.ISellerService.Trip> getTrips(string src, string dst, DateTime date, List<string> sellers)
        {
            Dictionary<string, Uri> machines = new Dictionary<string, Uri>();
            foreach (string seller in sellers)
            {
                machines[seller]
            }
            
        }
    }
}
