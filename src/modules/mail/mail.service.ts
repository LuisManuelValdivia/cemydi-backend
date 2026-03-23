import { Injectable, InternalServerErrorException, Logger } from '@nestjs/common';
import nodemailer, { type Transporter } from 'nodemailer';

@Injectable()
export class MailService {
  private readonly logger = new Logger(MailService.name);
  private transporter: Transporter | null = null;

  private getTransporter() {
    if (this.transporter) {
      return this.transporter;
    }

    const host = process.env.SMTP_HOST?.trim();
    const port = Number(process.env.SMTP_PORT ?? '587');
    const user = process.env.SMTP_USER?.trim();
    const pass = process.env.SMTP_PASS?.trim();
    const secure = `${process.env.SMTP_SECURE ?? ''}`.trim().toLowerCase() === 'true';

    if (!host || !user || !pass || Number.isNaN(port)) {
      throw new InternalServerErrorException(
        'SMTP no esta configurado completamente. Define SMTP_HOST, SMTP_PORT, SMTP_USER y SMTP_PASS para enviar correos.',
      );
    }

    this.transporter = nodemailer.createTransport({
      host,
      port,
      secure,
      auth: {
        user,
        pass,
      },
    });

    return this.transporter;
  }

  private getFromAddress() {
    return process.env.MAIL_FROM?.trim() || process.env.SMTP_USER?.trim() || 'no-reply@cemydi.local';
  }

  async sendEmailVerificationLink(input: { correo: string; nombre: string; verificationUrl: string }) {
    await this.sendMail({
      to: input.correo,
      subject: 'Verifica tu cuenta de CEMYDI',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.5;">
          <h2>Hola ${this.escapeHtml(input.nombre)}, confirma tu correo</h2>
          <p>Haz clic en el siguiente boton para verificar tu cuenta:</p>
          <p style="margin: 24px 0;">
            <a
              href="${input.verificationUrl}"
              style="display: inline-block; padding: 12px 20px; border-radius: 12px; background: #1e6260; color: #ffffff; text-decoration: none; font-weight: 700;"
            >
              Verificar mi cuenta
            </a>
          </p>
          <p>Si el boton no funciona, copia y pega este enlace en tu navegador:</p>
          <p><a href="${input.verificationUrl}">${input.verificationUrl}</a></p>
          <p>Si no solicitaste esta cuenta, puedes ignorar este mensaje.</p>
        </div>
      `,
      text: `Hola ${input.nombre}, verifica tu cuenta entrando a este enlace: ${input.verificationUrl}`,
    });
  }

  async sendPasswordResetCode(input: { correo: string; nombre: string; code: string }) {
    await this.sendMail({
      to: input.correo,
      subject: 'Código para restablecer tu contraseña en CEMYDI',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.5;">
          <h2>Hola ${this.escapeHtml(input.nombre)}</h2>
          <p>Tu código para restablecer la contraseña es:</p>
          <p style="font-size: 28px; font-weight: 700; letter-spacing: 6px;">${input.code}</p>
          <p>Este código expira en pocos minutos. Si no solicitaste el cambio, ignora este mensaje.</p>
        </div>
      `,
      text: `Hola ${input.nombre}, tu código para restablecer la contraseña es: ${input.code}`,
    });
  }

  private async sendMail(input: { to: string; subject: string; html: string; text: string }) {
    const transporter = this.getTransporter();
    try {
      await transporter.sendMail({
        from: this.getFromAddress(),
        to: input.to,
        subject: input.subject,
        html: input.html,
        text: input.text,
      });
    } catch (error) {
      this.logger.error(`No se pudo enviar el correo a ${input.to}`, error);
      throw new InternalServerErrorException(
        'No se pudo enviar el correo. Verifica la configuracion SMTP e intenta de nuevo.',
      );
    }
  }

  private escapeHtml(value: string) {
    return value
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }
}
