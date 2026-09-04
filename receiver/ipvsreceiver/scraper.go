// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ipvsreceiver // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/receiver/ipvsreceiver"

import (
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cloudflare/ipvs"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/receiver/ipvsreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type ipvsScraper struct {
	log    *zap.Logger
	mb     *metadata.MetricsBuilder
	client ipvs.Client
}

func newScraper(mbc metadata.MetricsBuilderConfig, set receiver.Settings) *ipvsScraper {
	return &ipvsScraper{
		log:    set.Logger,
		mb:     metadata.NewMetricsBuilder(mbc, set),
		client: nil,
	}
}

func (s *ipvsScraper) start(context.Context, component.Host) error {
	var err error
	s.client, err = ipvs.New()

	return err
}

func (s *ipvsScraper) shutdown(context.Context) error {
	if s.client != nil {
		if closer, ok := s.client.(io.Closer); ok {
			_ = closer.Close()
		}
		s.client = nil
	}

	return nil
}

func (s *ipvsScraper) scrape(context.Context) (pmetric.Metrics, error) {
	services, err := s.client.Services()
	if err != nil {
		s.log.Error("client.Services:", zap.Error(err))
		return s.mb.Emit(), err
	}

	ts := pcommon.NewTimestampFromTime(time.Now())

	for i := range services {
		if services[i].FWMark > 0 {
			continue
		}

		netmask := services[i].Netmask.String()
		protocol := metadata.MapAttributeProtocol[strings.ToLower(services[i].Protocol.String())]
		sched := metadata.MapAttributeSched[strings.ToLower(services[i].Scheduler)]

		vipAddress := services[i].Address.String()
		vipAddressFamily := metadata.MapAttributeVipFamily[strings.ToLower(services[i].Family.String())]
		vipPort := strconv.Itoa(int(services[i].Port))

		s.mb.RecordIpvsServiceTimeoutDataPoint(
			ts,
			int64(services[i].Timeout),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceConnectionTotalDataPoint(
			ts,
			int64(services[i].Stats.Connections),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServicePacketInTotalDataPoint(
			ts,
			int64(services[i].Stats.IncomingPackets),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServicePacketOutTotalDataPoint(
			ts,
			int64(services[i].Stats.OutgoingPackets),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceInTotalDataPoint(
			ts,
			int64(services[i].Stats.IncomingBytes),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceOutTotalDataPoint(
			ts,
			int64(services[i].Stats.OutgoingBytes),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServicePacketInRateDataPoint(
			ts,
			int64(services[i].Stats.IncomingPacketRate),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServicePacketOutRateDataPoint(
			ts,
			int64(services[i].Stats.OutgoingPacketRate),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceInRateDataPoint(
			ts,
			int64(services[i].Stats.IncomingByteRate),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceOutRateDataPoint(
			ts,
			int64(services[i].Stats.OutgoingByteRate),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)
		s.mb.RecordIpvsServiceConnectionRateDataPoint(
			ts,
			int64(services[i].Stats.ConnectionRate),
			netmask,
			protocol,
			sched,
			vipAddress,
			vipAddressFamily,
			vipPort,
		)

		destinations, err := s.client.Destinations(services[i].Service)
		if err != nil {
			s.log.Warn("client.Destinations:", zap.Error(err))
			continue
		}

		for j := range destinations {
			forwardType := metadata.MapAttributeForwardType[strings.ToLower(destinations[j].FwdMethod.String())]
			tunnelPort := strconv.Itoa(int(destinations[j].TunnelPort))
			tunnelType := metadata.MapAttributeTunnelType[strings.ToLower(destinations[j].TunnelType.String())]

			ripAddress := destinations[j].Address.String()
			ripAddressFamily := metadata.MapAttributeRipFamily[strings.ToLower(destinations[j].Family.String())]
			ripPort := strconv.Itoa(int(destinations[j].Port))

			s.mb.RecordIpvsDestinationConnectionWeightDataPoint(
				ts,
				int64(destinations[j].Weight),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationConnectionActiveCountDataPoint(
				ts,
				int64(destinations[j].ActiveConnections),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationConnectionInactiveCountDataPoint(
				ts,
				int64(destinations[j].InactiveConnections),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationConnectionPersistentCountDataPoint(
				ts,
				int64(destinations[j].PersistentConnections),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationConnectionTotalDataPoint(
				ts,
				int64(destinations[j].Stats.Connections),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationPacketInTotalDataPoint(
				ts,
				int64(destinations[j].Stats.IncomingPackets),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationPacketOutTotalDataPoint(
				ts,
				int64(destinations[j].Stats.OutgoingPackets),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationInTotalDataPoint(
				ts,
				int64(destinations[j].Stats.IncomingBytes),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationOutTotalDataPoint(
				ts,
				int64(destinations[j].Stats.OutgoingBytes),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationPacketInRateDataPoint(
				ts,
				int64(destinations[j].Stats.IncomingPacketRate),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationPacketOutRateDataPoint(
				ts,
				int64(destinations[j].Stats.OutgoingPacketRate),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationInRateDataPoint(
				ts,
				int64(destinations[j].Stats.IncomingByteRate),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationOutRateDataPoint(
				ts,
				int64(destinations[j].Stats.OutgoingByteRate),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
			s.mb.RecordIpvsDestinationConnectionRateDataPoint(
				ts,
				int64(destinations[j].Stats.ConnectionRate),
				netmask,
				protocol,
				sched,
				vipAddress,
				vipAddressFamily,
				vipPort,
				forwardType,
				tunnelPort,
				tunnelType,
				ripAddress,
				ripAddressFamily,
				ripPort,
			)
		}
	}

	return s.mb.Emit(), nil
}
