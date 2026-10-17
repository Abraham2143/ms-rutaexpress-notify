package cl.rutaexpress.ms_rutaexpress_notify.processing;

import cl.rutaexpress.ms_rutaexpress_notify.contract.Command;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.font.*;
import org.springframework.stereotype.Service;

@Service
public class LabelService {
    private final ArtifactStore store;
    public LabelService(ArtifactStore store) { this.store=store; }
    public void process(Command command) throws Exception {
        store.save("label", command.eventId(), ".pdf", path -> {
            try (var document=new PDDocument()) {
                var page=new PDPage(); document.addPage(page);
                var font=new PDType1Font(Standard14Fonts.FontName.HELVETICA);
                try (var content=new PDPageContentStream(document,page)) {
                    content.beginText(); content.setFont(font, 11); content.setLeading(16);
                    content.newLineAtOffset(40,750);
                    String text="RutaExpress - Etiqueta de envío\n" + WarehouseService.details(command);
                    for (String line:text.split("\n")) {
                        // Standard PDF fonts support Spanish; replace unsupported glyphs explicitly.
                        StringBuilder safe=new StringBuilder();
                        for (int cp:line.codePoints().toArray()) {
                            String glyph=new String(Character.toChars(cp));
                            try { font.encode(glyph); safe.append(glyph); }
                            catch (IllegalArgumentException e) { safe.append('?'); }
                        }
                        StringBuilder wrapped=new StringBuilder();
                        for (int i=0; i<safe.length(); i++) {
                            String next=wrapped.toString()+safe.charAt(i);
                            if (font.getStringWidth(next)/1000*11>520 && !wrapped.isEmpty()) {
                                content.showText(wrapped.toString()); content.newLine(); wrapped.setLength(0);
                            }
                            wrapped.append(safe.charAt(i));
                        }
                        content.showText(wrapped.toString()); content.newLine();
                    }
                    content.endText();
                }
                document.save(path.toFile());
            }
        });
    }
}
