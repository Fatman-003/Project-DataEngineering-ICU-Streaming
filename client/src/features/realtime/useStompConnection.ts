import { Client } from '@stomp/stompjs'
import type { IMessage, StompSubscription } from '@stomp/stompjs'
import { useEffect, useRef, useState } from 'react'
import type { RealtimeEvent } from './types'
import { stayDestinations } from './topics'

export type ConnectionStatus = 'connecting' | 'connected' | 'offline'

interface Options {
  url: string
  stayId: number | null
  onEvent: (event: RealtimeEvent) => void
}

export function useStompConnection({ url, stayId, onEvent }: Options): ConnectionStatus {
  const [status, setStatus] = useState<ConnectionStatus>('connecting')
  const [epoch, setEpoch] = useState(0)
  const clientRef = useRef<Client | null>(null)
  const onEventRef = useRef(onEvent)

  useEffect(() => {
    onEventRef.current = onEvent
  }, [onEvent])

  useEffect(() => {
    let disposed = false

    const client = new Client({
      brokerURL: url,
      reconnectDelay: 3000,
      heartbeatIncoming: 10_000,
      heartbeatOutgoing: 10_000,
      debug: () => undefined,
    })

    client.onConnect = () => {
      if (disposed) return
      setStatus('connected')
      setEpoch((value) => value + 1)
    }
    client.onDisconnect = () => {
      if (!disposed) setStatus('offline')
    }
    client.onWebSocketClose = () => {
      if (!disposed) setStatus('offline')
    }
    client.onStompError = () => {
      if (!disposed) setStatus('offline')
    }
    client.onWebSocketError = () => {
      if (!disposed) setStatus('offline')
    }

    client.activate()
    clientRef.current = client

    return () => {
      disposed = true
      clientRef.current = null
      void client.deactivate()
    }
  }, [url])

  useEffect(() => {
    const client = clientRef.current
    if (!client || status !== 'connected' || stayId === null) return

    const subscriptions: StompSubscription[] = stayDestinations(stayId).map((destination) =>
      client.subscribe(destination, (message) => {
        const event = parseEvent(message)
        if (event) onEventRef.current(event)
      }),
    )

    return () => {
      for (const subscription of subscriptions) {
        try {
          subscription.unsubscribe()
        } catch {
          // The connection may already be gone; nothing to clean up.
        }
      }
    }
  }, [status, epoch, stayId])

  return status
}

function parseEvent(message: IMessage): RealtimeEvent | null {
  try {
    const parsed: unknown = JSON.parse(message.body)
    if (typeof parsed !== 'object' || parsed === null) return null

    const event = parsed as Partial<RealtimeEvent>
    if (typeof event.type !== 'string' || typeof event.stayId !== 'number') return null

    return event as RealtimeEvent
  } catch {
    return null
  }
}
