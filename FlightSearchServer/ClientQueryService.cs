using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlightSearchServer
{
    class ClientQueryService: IClientQueryService
    {
        public QueryResultTrips GetFlights(string src, string dst, string date, string servers)
        {
            return FlightSearchLogic.Instance.QueryTrips(src, dst, date, servers);
            
        }
    }
}
