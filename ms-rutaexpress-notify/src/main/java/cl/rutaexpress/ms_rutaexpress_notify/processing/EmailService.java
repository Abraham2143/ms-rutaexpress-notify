package cl.rutaexpress.ms_rutaexpress_notify.processing;

import cl.rutaexpress.ms_rutaexpress_notify.contract.Command;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class EmailService {
    private final JavaMailSender sender;
    private final boolean enabled;
    private final String from;
    public EmailService(JavaMailSender sender, @Value("${notify.email.enabled:false}") boolean enabled,
                        @Value("${notify.email.from:notify@example.invalid}") String from) {
        this.sender=sender; this.enabled=enabled; this.from=from;
    }
    public void process(Command command) {
        if (!"EMAIL".equals(command.payload().channel()))
            throw new IllegalStateException("webpush_not_configured");
        if (!enabled) throw new IllegalStateException("smtp_channel_disabled");
        var p = command.payload();
        String state = switch(p.status()) {
            case "ACEPTADO" -> "Tu envío fue aceptado y será preparado.";
            case "EN_RUTA" -> "Tu envío está en ruta hacia su destino.";
            case "ENTREGADO" -> "Tu envío fue entregado.";
            default -> throw new IllegalArgumentException("unsupported_status");
        };
        var mail = new SimpleMailMessage();
        mail.setFrom(from); mail.setTo(p.destinatarioEmail());
        mail.setSubject("RutaExpress - " + p.trackingNumber() + " - " + p.status());
        // The flat Shipments notification carries no address; the v1 envelope always does.
        String destination = p.direccionDestino() == null ? "" : "\nDestino: " + p.direccionDestino();
        mail.setText("Hola " + p.destinatarioNombre() + ",\n\n" + state +
                "\nTracking: " + p.trackingNumber() + destination + "\nRutaExpress");
        sender.send(mail);
    }
}
