using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Net;

namespace FlightSearchServer
{
    [Serializable]
    public class FlightSearchServerException : Exception
    {
        public HttpStatusCode StatusCode = HttpStatusCode.NotFound; /* Legacy */
        public string StatusDescription = "request was not found**";

        public FlightSearchServerException()
            : base() { }

        public FlightSearchServerException(string message)
            : base(message) { }

        public FlightSearchServerException(string format, params object[] args)
            : base(string.Format(format, args)) { }

        public FlightSearchServerException(string message, Exception innerException)
            : base(message, innerException) { }

        public FlightSearchServerException(string format, Exception innerException, params object[] args)
            : base(string.Format(format, args), innerException) { }

        protected FlightSearchServerException(SerializationInfo info, StreamingContext context)
            : base(info, context) { }

    }

    public class FlightSearchServerSellerDropped : FlightSearchServerException
    {
        public FlightSearchServerSellerDropped()
            : base()
        {
            StatusCode = HttpStatusCode.BadGateway;
            StatusDescription = "seller not found";
        }

    }

    public class FlightSearchServerSellerNotFound : FlightSearchServerException
    {
        public FlightSearchServerSellerNotFound()
        {
            StatusCode = HttpStatusCode.NotFound;
            StatusDescription = "seller not found";
        }
    }

    public class FlightSearchServerEmptyQueryResult : FlightSearchServerException
    {
        public FlightSearchServerEmptyQueryResult()
        {
            StatusCode = HttpStatusCode.NotFound;
            StatusDescription = "no flights match your query";
        }
    }

    public class FlightSearchServerFlightNotFound : FlightSearchServerException
    {
        public FlightSearchServerFlightNotFound()
        {
            StatusCode = HttpStatusCode.NotFound;
            StatusDescription = "no such flight";
        }
    }

    public class FlightSearchServerBadDate : FlightSearchServerException
    {
        public FlightSearchServerBadDate()
        {
            StatusCode = HttpStatusCode.BadRequest;
            StatusDescription = "Invalid date format, please use dd/MM/yyyy format";
        }
    }

    public class FlightSearchServerReservationNotFound : FlightSearchServerException
    {
        public FlightSearchServerReservationNotFound()
        {
            StatusCode = HttpStatusCode.NotFound;
            StatusDescription = "no such reservation";
        }
    }

    public class FlightSearchServerReservationMalformed : FlightSearchServerException
    {
        public FlightSearchServerReservationMalformed()
        {
            StatusCode = HttpStatusCode.BadRequest;
            StatusDescription = "Invalid reservation ID";
        }
    }

    public class FlightSearchServerNoSeats : FlightSearchServerException
    {
        public FlightSearchServerNoSeats()
        {
            StatusCode = HttpStatusCode.Forbidden;
            StatusDescription = "no seats available";
        }
    }
}
