package cl.rutaexpress.ms_rutaexpress_notify.processing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import cl.rutaexpress.ms_rutaexpress_notify.contract.Command;
import org.springframework.stereotype.Service;

@Service
public class WarehouseService {
    private final ArtifactStore store;
    public WarehouseService(ArtifactStore store) { this.store=store; }
    public void process(Command command) throws Exception {
        store.save("warehouse", command.eventId(), ".txt", path -> Files.writeString(path,
                "RutaExpress - Ticket de picking\n" + details(command), StandardCharsets.UTF_8));
    }
    static String details(Command c) {
        var p=c.payload();
        return "Evento: " + c.eventId() + "\nFecha: " + c.timestamp() + "\nEnvío: " + p.id() +
                "\nTracking: " + p.trackingNumber() + "\nDestinatario: " + p.destinatarioNombre() +
                "\nOrigen: " + p.direccionOrigen() + "\nDestino: " + p.direccionDestino() +
                "\nServicio: " + p.servicioId() + "\nEstado: " + p.status() + "\n";
    }
}
