package cl.rutaexpress.ms_rutaexpress_notify.processing;

import java.nio.file.*;
import java.io.IOException;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ArtifactStore {
    private final Path root;
    public ArtifactStore(@Value("${notify.storage.path:./data}") String path) { root=Path.of(path).toAbsolutePath().normalize(); }
    @FunctionalInterface public interface Writer { void write(Path path) throws IOException; }
    public void save(String flow, UUID eventId, String suffix, Writer writer) throws IOException {
        if (!java.util.Set.of("warehouse", "label").contains(flow) || !java.util.Set.of(".txt", ".pdf").contains(suffix))
            throw new IllegalArgumentException("invalid_artifact_kind");
        Path directory=root.resolve(flow);
        Files.createDirectories(directory);
        Path target=directory.resolve(eventId.toString()+suffix);
        if (Files.isSymbolicLink(directory) || Files.isSymbolicLink(target)) throw new IOException("unsafe_storage");
        if (Files.exists(target)) return;
        Path temporary=Files.createTempFile(directory, ".pending-", ".tmp");
        try {
            writer.write(temporary);
            Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);
        } finally { Files.deleteIfExists(temporary); }
    }
}
