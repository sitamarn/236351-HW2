using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Web;

namespace FlightSearchServer
{
    /// <summary>
    /// Defines the API between Client and Search server
    /// </summary>
    [ServiceContract]
    public interface IClientQueryService
    {
        /// <summary>
        /// See concrete class for details
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <param name="date"></param>
        /// <returns></returns>
        [WebGet(UriTemplate = "flight?src={src}&dst={dst}&date={date}&servers={servers}")]
        [OperationContract]
        QueryResultTrips GetFlights(string src, string dst, string date, string servers);

    }
}